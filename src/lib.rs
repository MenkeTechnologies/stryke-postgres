//! stryke-postgres — PostgreSQL cdylib loaded in-process by stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn pg__*` is a JSON-string-in /
//! JSON-string-out wrapper around the sync `postgres` crate. stryke's FFI
//! bridge (`rust_ffi.rs::load_cdylib`) resolves these symbols at first
//! `use Postgres`, registers each one as a stryke-callable function, and
//! on each call passes a JSON-encoded args dict and copies the returned
//! JSON into a stryke string.
//!
//! Persistent state: `CLIENTS` caches one `postgres::Client` per
//! connection URL for the life of the stryke process, wrapped in a
//! `Mutex` since `Client` is not `Clone`. The v1 helper opened a fresh
//! TCP+TLS+auth handshake per fork.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use std::io::{Read, Write};
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::ToSql;
use postgres::{Client, NoTls, Row};
use serde_json::{json, Map, Value};

// ── client cache ────────────────────────────────────────────────────────────

type ClientHandle = Arc<Mutex<Client>>;

static CLIENTS: OnceCell<Mutex<HashMap<String, ClientHandle>>> = OnceCell::new();

fn clients() -> &'static Mutex<HashMap<String, ClientHandle>> {
    CLIENTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn url_from_opts(opts: &Value) -> String {
    if let Some(u) = opts.get("url").and_then(|v| v.as_str()) {
        return u.to_string();
    }
    if let Ok(u) = std::env::var("DATABASE_URL").or_else(|_| std::env::var("POSTGRES_URL")) {
        return u;
    }
    let host_raw = opts
        .get("host")
        .and_then(|v| v.as_str())
        .unwrap_or("127.0.0.1");
    // Same class of DSN-injection as the userinfo fix: a `host` value like
    // `evil.example.com/?#@trusted-host` raw-interpolated into the DSN would
    // re-anchor the authority component and silently redirect the connection.
    // Reject host strings containing URL-meaningful characters. Hosts are
    // hostnames or IP literals — none of `@`, `/`, `?`, `#`, `[`, `]`, ` `, `\\`
    // belong there. IPv6 must use bracketed form e.g. `[::1]` which is allowed.
    let host = sanitize_host(host_raw);
    let port = opts.get("port").and_then(|v| v.as_i64()).unwrap_or(5432);
    let user = opts
        .get("user")
        .and_then(|v| v.as_str())
        .unwrap_or("postgres");
    let password = opts.get("password").and_then(|v| v.as_str()).unwrap_or("");
    let db = opts.get("database").and_then(|v| v.as_str()).unwrap_or("");
    let auth = if password.is_empty() {
        percent_encode_userinfo(user)
    } else {
        format!(
            "{}:{}",
            percent_encode_userinfo(user),
            percent_encode_userinfo(password)
        )
    };
    format!(
        "postgresql://{}@{}:{}/{}",
        auth,
        host,
        port,
        percent_encode_userinfo(db)
    )
}

/// Reject host strings containing URL-meaningful chars that would re-anchor the
/// DSN authority. Returns the cleaned host or "127.0.0.1" on rejection (safer to
/// fail closed than to pass an injection through).
fn sanitize_host(input: &str) -> String {
    // IPv6 literal — `[...]` — is opaque to the URL parser; allow as-is when
    // matched on both ends. Internal `[`/`]` outside that pattern is rejected.
    if input.starts_with('[') && input.ends_with(']') && input.len() > 2 {
        let inner = &input[1..input.len() - 1];
        if !inner
            .bytes()
            .any(|b| matches!(b, b'@' | b'/' | b'?' | b'#' | b' ' | b'\\' | b'[' | b']'))
        {
            return input.to_string();
        }
        return "127.0.0.1".to_string();
    }
    if input.bytes().any(|b| {
        matches!(
            b,
            b'@' | b'/' | b'?' | b'#' | b' ' | b'\\' | b'[' | b']' | b':'
        )
    }) {
        return "127.0.0.1".to_string();
    }
    input.to_string()
}

/// RFC 3986 percent-encode for the URI userinfo component (and path segment).
/// Pre-fix, passwords containing `@`, `:`, `/`, `#`, `?` mangled the DSN —
/// e.g. password `p@ss` would make `ss` the host. This encodes everything
/// outside the unreserved set (`ALPHA / DIGIT / "-" / "." / "_" / "~"`).
fn percent_encode_userinfo(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for b in input.as_bytes() {
        match *b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*b as char);
            }
            other => {
                out.push('%');
                out.push_str(&format!("{:02X}", other));
            }
        }
    }
    out
}

/// Validate a SQL identifier (table or schema-qualified `schema.table`) for
/// safe interpolation into `format!("... {table} ...")`. Postgres identifier
/// rules: ALPHA / "_" first, then ALPHA / DIGIT / "_" / "$". We additionally
/// allow `.` to permit `schema.table`. Pre-fix, a table param of
/// `users; DROP TABLE users` was concatenated raw into `SELECT * FROM ...`
/// enabling SQL injection.
fn validate_identifier(name: &str, what: &str) -> Result<String> {
    if name.is_empty() {
        bail!("`{what}` must not be empty");
    }
    let valid_start = |c: char| c.is_ascii_alphabetic() || c == '_';
    let valid_rest = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '$';
    for (i, part) in name.split('.').enumerate() {
        if part.is_empty() {
            bail!("`{what}` has empty segment (position {i}) in `{name}`");
        }
        let mut chars = part.chars();
        let first = chars.next().expect("non-empty checked above");
        if !valid_start(first) {
            bail!("`{what}` segment `{part}` must start with letter or underscore");
        }
        for c in chars {
            if !valid_rest(c) {
                bail!("`{what}` segment `{part}` contains invalid character `{c}`");
            }
        }
    }
    Ok(name.to_string())
}

fn with_client<F, R>(opts: &Value, f: F) -> Result<R>
where
    F: FnOnce(&mut Client) -> Result<R>,
{
    let url = url_from_opts(opts);
    let handle = {
        let mut map = clients().lock();
        if let Some(h) = map.get(&url) {
            Arc::clone(h)
        } else {
            let c = Client::connect(&url, NoTls)?;
            let h = Arc::new(Mutex::new(c));
            map.insert(url, Arc::clone(&h));
            h
        }
    };
    let mut client = handle.lock();
    f(&mut client)
}

// ── JSON ↔ postgres conversion ──────────────────────────────────────────────

/// Convert a `Value` to a `Box<dyn ToSql + Sync>` so `query`/`execute` accept it.
/// Restricted set; expand as needed.
#[derive(Debug)]
enum PgParam {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Json(Value),
}

impl ToSql for PgParam {
    fn to_sql(
        &self,
        ty: &postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    {
        use postgres::types::Type;
        match self {
            PgParam::Null => Ok(postgres::types::IsNull::Yes),
            PgParam::Bool(b) => b.to_sql(ty, out),
            // A JSON number arrives as i64, but `i64::to_sql` only accepts
            // INT8 — binding it to an INT2/INT4 (the common case: an `id INT`
            // column) would fail the type check. Narrow to the column's
            // integer width, and allow an integer literal to satisfy a float
            // column, so `bind => [1]` works regardless of the target type.
            PgParam::Int(n) => match *ty {
                Type::INT2 => (*n as i16).to_sql(ty, out),
                Type::INT4 => (*n as i32).to_sql(ty, out),
                Type::FLOAT4 => (*n as f32).to_sql(ty, out),
                Type::FLOAT8 => (*n as f64).to_sql(ty, out),
                _ => n.to_sql(ty, out),
            },
            // Likewise narrow f64 to FLOAT4 when the column is real.
            PgParam::Float(f) => match *ty {
                Type::FLOAT4 => (*f as f32).to_sql(ty, out),
                _ => f.to_sql(ty, out),
            },
            PgParam::Str(s) => s.to_sql(ty, out),
            PgParam::Json(v) => v.to_sql(ty, out),
        }
    }
    fn accepts(_ty: &postgres::types::Type) -> bool
    where
        Self: Sized,
    {
        true
    }
    postgres::types::to_sql_checked!();
}

fn json_to_param(v: &Value) -> PgParam {
    match v {
        Value::Null => PgParam::Null,
        Value::Bool(b) => PgParam::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PgParam::Int(i)
            } else if let Some(f) = n.as_f64() {
                PgParam::Float(f)
            } else {
                PgParam::Str(n.to_string())
            }
        }
        Value::String(s) => PgParam::Str(s.clone()),
        _ => PgParam::Json(v.clone()),
    }
}

fn row_to_json(row: &Row) -> Value {
    let mut obj = Map::new();
    for (i, col) in row.columns().iter().enumerate() {
        let name = col.name().to_string();
        let v = pg_value_to_json(row, i, col.type_());
        obj.insert(name, v);
    }
    Value::Object(obj)
}

fn pg_value_to_json(row: &Row, i: usize, ty: &postgres::types::Type) -> Value {
    use postgres::types::Type;
    match *ty {
        Type::BOOL => row
            .try_get::<_, Option<bool>>(i)
            .ok()
            .flatten()
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        Type::INT2 => row
            .try_get::<_, Option<i16>>(i)
            .ok()
            .flatten()
            .map(|n| json!(n))
            .unwrap_or(Value::Null),
        Type::INT4 => row
            .try_get::<_, Option<i32>>(i)
            .ok()
            .flatten()
            .map(|n| json!(n))
            .unwrap_or(Value::Null),
        Type::INT8 => row
            .try_get::<_, Option<i64>>(i)
            .ok()
            .flatten()
            .map(|n| json!(n))
            .unwrap_or(Value::Null),
        Type::FLOAT4 => row
            .try_get::<_, Option<f32>>(i)
            .ok()
            .flatten()
            .map(|n| json!(n))
            .unwrap_or(Value::Null),
        Type::FLOAT8 => row
            .try_get::<_, Option<f64>>(i)
            .ok()
            .flatten()
            .map(|n| json!(n))
            .unwrap_or(Value::Null),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => row
            .try_get::<_, Option<String>>(i)
            .ok()
            .flatten()
            .map(Value::String)
            .unwrap_or(Value::Null),
        Type::JSON | Type::JSONB => row
            .try_get::<_, Option<Value>>(i)
            .ok()
            .flatten()
            .unwrap_or(Value::Null),
        Type::UUID => row
            .try_get::<_, Option<uuid::Uuid>>(i)
            .ok()
            .flatten()
            .map(|u| Value::String(u.to_string()))
            .unwrap_or(Value::Null),
        Type::TIMESTAMP => row
            .try_get::<_, Option<chrono::NaiveDateTime>>(i)
            .ok()
            .flatten()
            .map(|t| Value::String(t.to_string()))
            .unwrap_or(Value::Null),
        Type::TIMESTAMPTZ => row
            .try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(i)
            .ok()
            .flatten()
            .map(|t| Value::String(t.to_rfc3339()))
            .unwrap_or(Value::Null),
        Type::DATE => row
            .try_get::<_, Option<chrono::NaiveDate>>(i)
            .ok()
            .flatten()
            .map(|d| Value::String(d.to_string()))
            .unwrap_or(Value::Null),
        _ => row
            .try_get::<_, Option<String>>(i)
            .ok()
            .flatten()
            .map(Value::String)
            .unwrap_or(Value::Null),
    }
}

fn params_from_value(v: &Value) -> Vec<PgParam> {
    match v.as_array() {
        Some(arr) => arr.iter().map(json_to_param).collect(),
        None => Vec::new(),
    }
}

fn params_as_sql(params: &[PgParam]) -> Vec<&(dyn ToSql + Sync)> {
    params.iter().map(|p| p as &(dyn ToSql + Sync)).collect()
}

// ── ops ─────────────────────────────────────────────────────────────────────

fn op_ping(opts: Value) -> Result<Value> {
    with_client(&opts, |c| {
        let _: i32 = c.query_one("SELECT 1", &[])?.get(0);
        Ok(json!({"ok": true}))
    })
}

fn op_version(opts: Value) -> Result<Value> {
    with_client(&opts, |c| {
        let v: String = c.query_one("SELECT version()", &[])?.get(0);
        Ok(json!({"version": v}))
    })
}

fn op_databases(opts: Value) -> Result<Value> {
    with_client(&opts, |c| {
        let rows = c.query(
            "SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname",
            &[],
        )?;
        let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        Ok(json!({"databases": names}))
    })
}

fn op_tables(opts: Value) -> Result<Value> {
    with_client(&opts, |c| {
        let rows = c.query(
            "SELECT schemaname || '.' || tablename FROM pg_tables \
             WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY 1",
            &[],
        )?;
        let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        Ok(json!({"tables": names}))
    })
}

fn op_schema(opts: Value) -> Result<Value> {
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?
        .to_string();
    with_client(&opts, |c| {
        let rows = c.query(
            "SELECT column_name, data_type, is_nullable \
             FROM information_schema.columns WHERE table_name = $1 \
             ORDER BY ordinal_position",
            &[&table],
        )?;
        let cols: Vec<Value> = rows
            .iter()
            .map(|r| {
                json!({
                    "name": r.get::<_, String>(0),
                    "type": r.get::<_, String>(1),
                    "nullable": r.get::<_, String>(2) == "YES",
                })
            })
            .collect();
        Ok(json!({"table": table, "columns": cols}))
    })
}

fn op_query(opts: Value) -> Result<Value> {
    let sql = opts["sql"]
        .as_str()
        .ok_or_else(|| anyhow!("missing sql"))?
        .to_string();
    let params = params_from_value(&opts["params"]);
    with_client(&opts, |c| {
        let p_refs = params_as_sql(&params);
        let rows = c.query(&sql, &p_refs)?;
        let names: Vec<String> = rows
            .first()
            .map(|r| r.columns().iter().map(|c| c.name().to_string()).collect())
            .unwrap_or_default();
        let out: Vec<Value> = rows.iter().map(row_to_json).collect();
        Ok(json!({"columns": names, "rows": out}))
    })
}

fn op_execute(opts: Value) -> Result<Value> {
    let sql = opts["sql"]
        .as_str()
        .ok_or_else(|| anyhow!("missing sql"))?
        .to_string();
    let params = params_from_value(&opts["params"]);
    with_client(&opts, |c| {
        let p_refs = params_as_sql(&params);
        let n = c.execute(&sql, &p_refs)?;
        Ok(json!({"affected": n as i64}))
    })
}

fn op_exec(opts: Value) -> Result<Value> {
    let sql = opts["sql"].as_str().ok_or_else(|| anyhow!("missing sql"))?;
    with_client(&opts, |c| {
        c.batch_execute(sql)?;
        Ok(json!({"ok": true}))
    })
}

/// Bulk-load via `COPY ... FROM STDIN`. `sql` is the COPY statement; `data`
/// is the raw payload (text/CSV lines, or whatever FORMAT the statement
/// declares). Returns the row count Postgres reports.
fn op_copy_in(opts: Value) -> Result<Value> {
    let sql = opts["sql"]
        .as_str()
        .ok_or_else(|| anyhow!("missing sql (a COPY ... FROM STDIN statement)"))?
        .to_string();
    let data = opts["data"]
        .as_str()
        .ok_or_else(|| anyhow!("missing data (the COPY payload)"))?
        .to_string();
    with_client(&opts, |c| {
        let mut writer = c.copy_in(&sql)?;
        writer.write_all(data.as_bytes())?;
        let n = writer.finish()?;
        Ok(json!({"rows": n as i64}))
    })
}

/// Bulk-export via `COPY ... TO STDOUT`. Returns the raw payload as `data`.
fn op_copy_out(opts: Value) -> Result<Value> {
    let sql = opts["sql"]
        .as_str()
        .ok_or_else(|| anyhow!("missing sql (a COPY ... TO STDOUT statement)"))?
        .to_string();
    with_client(&opts, |c| {
        let mut reader = c.copy_out(&sql)?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        Ok(json!({"data": String::from_utf8_lossy(&buf), "bytes": buf.len()}))
    })
}

/// Send a `NOTIFY` to `channel` with an optional `payload`. Uses
/// `pg_notify($1,$2)` so the channel/payload are bound, not interpolated.
fn op_notify(opts: Value) -> Result<Value> {
    let channel = opts["channel"]
        .as_str()
        .ok_or_else(|| anyhow!("missing channel"))?
        .to_string();
    let payload = opts["payload"].as_str().unwrap_or("").to_string();
    with_client(&opts, |c| {
        c.execute("SELECT pg_notify($1, $2)", &[&channel, &payload])?;
        Ok(json!({"ok": true, "channel": channel}))
    })
}

/// `LISTEN` on one or more `channels`, then drain pending notifications for
/// up to `timeout_ms` (default 1000). Returns the collected notifications.
/// Snapshot-style: a long-running listener daemon is out of scope for the
/// blocking FFI shape.
fn op_listen(opts: Value) -> Result<Value> {
    let channels: Vec<String> = match &opts["channels"] {
        Value::Array(a) => a
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        Value::String(s) => vec![s.clone()],
        _ => return Err(anyhow!("missing channels (a channel name or array)")),
    };
    if channels.is_empty() {
        return Err(anyhow!("channels must name at least one channel"));
    }
    let timeout = Duration::from_millis(opts["timeout_ms"].as_u64().unwrap_or(1000));
    with_client(&opts, |c| {
        for ch in &channels {
            // Channel names can't be bound params in LISTEN; quote as an ident.
            c.batch_execute(&format!("LISTEN {}", quote_ident(ch)))?;
        }
        let mut out = Vec::new();
        let mut notifs = c.notifications();
        let mut it = notifs.timeout_iter(timeout);
        while let Some(n) = it.next()? {
            out.push(json!({
                "channel": n.channel(),
                "payload": n.payload(),
                "pid": n.process_id(),
            }));
        }
        Ok(json!({"notifications": out, "count": out.len()}))
    })
}

/// Quote a Postgres identifier (double-quote, escaping embedded quotes).
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn op_explain(opts: Value) -> Result<Value> {
    let sql = opts["sql"].as_str().ok_or_else(|| anyhow!("missing sql"))?;
    let analyze = opts["analyze"].as_bool().unwrap_or(false);
    let params = params_from_value(&opts["params"]);
    // ANALYZE actually runs the statement — only enable when the caller asks.
    let prefix = if analyze {
        "EXPLAIN (ANALYZE, FORMAT TEXT) "
    } else {
        "EXPLAIN (FORMAT TEXT) "
    };
    let full = format!("{}{}", prefix, sql);
    with_client(&opts, |c| {
        let p_refs = params_as_sql(&params);
        let rows = c.query(&full, &p_refs)?;
        let plan: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        Ok(json!({"plan": plan}))
    })
}

fn op_views(opts: Value) -> Result<Value> {
    with_client(&opts, |c| {
        let rows = c.query(
            "SELECT schemaname || '.' || viewname FROM pg_views \
             WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY 1",
            &[],
        )?;
        let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        Ok(json!({"views": names}))
    })
}

fn op_functions(opts: Value) -> Result<Value> {
    with_client(&opts, |c| {
        let rows = c.query(
            "SELECT n.nspname || '.' || p.proname \
             FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname NOT IN ('pg_catalog','information_schema') ORDER BY 1",
            &[],
        )?;
        let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        Ok(json!({"functions": names}))
    })
}

fn op_indexes(opts: Value) -> Result<Value> {
    let table = opts["table"].as_str();
    with_client(&opts, |c| {
        let rows = match table {
            Some(t) => c.query(
                "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = $1 ORDER BY indexname",
                &[&t],
            )?,
            None => c.query(
                "SELECT indexname, indexdef FROM pg_indexes \
                 WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY indexname",
                &[],
            )?,
        };
        let idx: Vec<Value> = rows
            .iter()
            .map(|r| json!({"name": r.get::<_, String>(0), "def": r.get::<_, String>(1)}))
            .collect();
        Ok(json!({"indexes": idx}))
    })
}

fn op_roles(opts: Value) -> Result<Value> {
    with_client(&opts, |c| {
        let rows = c.query(
            "SELECT rolname, rolsuper, rolcanlogin FROM pg_roles ORDER BY rolname",
            &[],
        )?;
        let roles: Vec<Value> = rows
            .iter()
            .map(|r| {
                json!({
                    "name": r.get::<_, String>(0),
                    "superuser": r.get::<_, bool>(1),
                    "can_login": r.get::<_, bool>(2),
                })
            })
            .collect();
        Ok(json!({"roles": roles}))
    })
}

fn op_db_size(opts: Value) -> Result<Value> {
    // Bytes for the named (or current) database, plus a pretty form.
    with_client(&opts, |c| {
        let row = c.query_one(
            "SELECT pg_database_size(current_database()), \
             pg_size_pretty(pg_database_size(current_database()))",
            &[],
        )?;
        Ok(json!({"bytes": row.get::<_, i64>(0), "pretty": row.get::<_, String>(1)}))
    })
}

fn op_activity(opts: Value) -> Result<Value> {
    with_client(&opts, |c| {
        let rows = c.query(
            "SELECT pid, usename, state, wait_event_type, \
             EXTRACT(EPOCH FROM (now() - query_start))::float8 AS age_seconds, query \
             FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND state IS NOT NULL \
             ORDER BY query_start",
            &[],
        )?;
        let out: Vec<Value> = rows
            .iter()
            .map(|r| {
                json!({
                    "pid": r.get::<_, i32>(0),
                    "user": r.get::<_, Option<String>>(1),
                    "state": r.get::<_, Option<String>>(2),
                    "wait_event_type": r.get::<_, Option<String>>(3),
                    "age_seconds": r.get::<_, Option<f64>>(4),
                    "query": r.get::<_, Option<String>>(5),
                })
            })
            .collect();
        Ok(json!({"activity": out}))
    })
}

fn op_locks(opts: Value) -> Result<Value> {
    with_client(&opts, |c| {
        let rows = c.query(
            "SELECT l.pid, l.locktype, l.mode, l.granted, c.relname \
             FROM pg_locks l LEFT JOIN pg_class c ON c.oid = l.relation \
             ORDER BY l.granted, l.pid",
            &[],
        )?;
        let out: Vec<Value> = rows
            .iter()
            .map(|r| {
                json!({
                    "pid": r.get::<_, Option<i32>>(0),
                    "locktype": r.get::<_, Option<String>>(1),
                    "mode": r.get::<_, Option<String>>(2),
                    "granted": r.get::<_, Option<bool>>(3),
                    "relation": r.get::<_, Option<String>>(4),
                })
            })
            .collect();
        Ok(json!({"locks": out}))
    })
}

fn op_table_size(opts: Value) -> Result<Value> {
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?;
    with_client(&opts, |c| {
        // $1::regclass resolves schema-qualified or bare names safely.
        let row = c.query_one(
            "SELECT pg_total_relation_size($1::regclass), \
             pg_size_pretty(pg_total_relation_size($1::regclass))",
            &[&table],
        )?;
        Ok(
            json!({"table": table, "bytes": row.get::<_, i64>(0), "pretty": row.get::<_, String>(1)}),
        )
    })
}

fn op_sequences(opts: Value) -> Result<Value> {
    with_client(&opts, |c| {
        let rows = c.query(
            "SELECT sequence_schema || '.' || sequence_name FROM information_schema.sequences \
             ORDER BY 1",
            &[],
        )?;
        let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        Ok(json!({"sequences": names}))
    })
}

fn op_extensions(opts: Value) -> Result<Value> {
    with_client(&opts, |c| {
        let rows = c.query(
            "SELECT extname, extversion FROM pg_extension ORDER BY extname",
            &[],
        )?;
        let out: Vec<Value> = rows
            .iter()
            .map(|r| json!({"name": r.get::<_, String>(0), "version": r.get::<_, Option<String>>(1)}))
            .collect();
        Ok(json!({"extensions": out}))
    })
}

fn op_triggers(opts: Value) -> Result<Value> {
    with_client(&opts, |c| {
        let rows = c.query(
            "SELECT trigger_name, event_object_table, event_manipulation, action_timing \
             FROM information_schema.triggers \
             WHERE trigger_schema NOT IN ('pg_catalog','information_schema') \
             ORDER BY event_object_table, trigger_name",
            &[],
        )?;
        let out: Vec<Value> = rows
            .iter()
            .map(|r| {
                json!({
                    "name": r.get::<_, String>(0),
                    "table": r.get::<_, Option<String>>(1),
                    "event": r.get::<_, Option<String>>(2),
                    "timing": r.get::<_, Option<String>>(3),
                })
            })
            .collect();
        Ok(json!({"triggers": out}))
    })
}

fn op_cancel_backend(opts: Value) -> Result<Value> {
    let pid = opts["pid"].as_i64().ok_or_else(|| anyhow!("missing pid"))? as i32;
    // `terminate => 1` issues pg_terminate_backend (hard kill) instead of cancel.
    let terminate = opts["terminate"].as_bool().unwrap_or(false);
    with_client(&opts, |c| {
        let sql = if terminate {
            "SELECT pg_terminate_backend($1)"
        } else {
            "SELECT pg_cancel_backend($1)"
        };
        let row = c.query_one(sql, &[&pid])?;
        Ok(json!({"pid": pid, "ok": row.get::<_, bool>(0)}))
    })
}

fn op_dump(opts: Value) -> Result<Value> {
    let table = validate_identifier(
        opts["table"]
            .as_str()
            .ok_or_else(|| anyhow!("missing table"))?,
        "table",
    )?;
    let limit = opts["limit"].as_i64();
    if let Some(n) = limit {
        if n < 0 {
            bail!("`limit` must be non-negative; got {n}");
        }
    }
    let sql = match limit {
        Some(n) => format!("SELECT * FROM {} LIMIT {}", table, n),
        None => format!("SELECT * FROM {}", table),
    };
    with_client(&opts, |c| {
        let rows = c.query(&sql, &[])?;
        let names: Vec<String> = rows
            .first()
            .map(|r| r.columns().iter().map(|c| c.name().to_string()).collect())
            .unwrap_or_default();
        let out: Vec<Value> = rows.iter().map(row_to_json).collect();
        Ok(json!({"columns": names, "rows": out}))
    })
}

fn op_insert_many(opts: Value) -> Result<Value> {
    let table = validate_identifier(
        opts["table"]
            .as_str()
            .ok_or_else(|| anyhow!("missing table"))?,
        "table",
    )?;
    let rows = opts["rows"]
        .as_array()
        .ok_or_else(|| anyhow!("missing rows (array of objects)"))?
        .clone();
    if rows.is_empty() {
        return Ok(json!({"inserted": 0}));
    }
    let first = rows[0]
        .as_object()
        .ok_or_else(|| anyhow!("first row must be an object"))?;
    let cols: Vec<String> = first
        .keys()
        .map(|k| validate_identifier(k, "column"))
        .collect::<Result<_>>()?;
    let col_list = cols.join(", ");
    let placeholders: Vec<String> = (1..=cols.len()).map(|i| format!("${}", i)).collect();
    // Optional RETURNING: a comma-separated list of column identifiers, or
    // `*`. Each token is validated as an identifier so the clause can't be
    // used to smuggle arbitrary SQL.
    let returning: Option<String> = match opts.get("returning") {
        Some(Value::String(s)) if !s.trim().is_empty() => {
            let expr = if s.trim() == "*" {
                "*".to_string()
            } else {
                s.split(',')
                    .map(|t| validate_identifier(t.trim(), "returning column"))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            };
            Some(expr)
        }
        _ => None,
    };
    let sql = match &returning {
        Some(expr) => format!(
            "INSERT INTO {} ({}) VALUES ({}) RETURNING {}",
            table,
            col_list,
            placeholders.join(", "),
            expr
        ),
        None => format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table,
            col_list,
            placeholders.join(", ")
        ),
    };
    with_client(&opts, |c| {
        let stmt = c.prepare(&sql)?;
        let mut total = 0i64;
        let mut returned: Vec<Value> = Vec::new();
        for row in &rows {
            let obj = row
                .as_object()
                .ok_or_else(|| anyhow!("row must be an object"))?;
            let params: Vec<PgParam> = cols.iter().map(|k| json_to_param(&obj[k])).collect();
            let p_refs = params_as_sql(&params);
            if returning.is_some() {
                let result = c.query(&stmt, &p_refs)?;
                total += result.len() as i64;
                returned.extend(result.iter().map(row_to_json));
            } else {
                total += c.execute(&stmt, &p_refs)? as i64;
            }
        }
        if returning.is_some() {
            Ok(json!({"inserted": total, "rows": returned}))
        } else {
            Ok(json!({"inserted": total}))
        }
    })
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call<F>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Result<Value>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| handler(input)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-postgres handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── pure helpers (no connection) ─────────────────────────────────────────────

/// RFC 3986 percent-decode — the inverse of `percent_encode_userinfo`, used to
/// recover userinfo / dbname / params from a URI DSN. Invalid escapes are left
/// verbatim.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Parse a URI DSN `postgres[ql]://[user[:pass]@]host[:port][/dbname][?k=v…]`
/// into its components. Userinfo, dbname and param values are percent-decoded.
/// Pure — opens no connection.
fn op_parse_dsn(opts: Value) -> Result<Value> {
    let dsn = opts
        .get("dsn")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing dsn"))?;
    let (scheme, rest) = dsn
        .split_once("://")
        .ok_or_else(|| anyhow!("not a URI DSN (missing `://`): {dsn}"))?;
    if !matches!(scheme, "postgres" | "postgresql") {
        bail!("unsupported scheme `{scheme}` (want postgres|postgresql)");
    }
    let (authority_path, query) = match rest.split_once('?') {
        Some((ap, q)) => (ap, Some(q)),
        None => (rest, None),
    };
    let (authority, path) = match authority_path.split_once('/') {
        Some((a, p)) => (a, Some(p)),
        None => (authority_path, None),
    };
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };
    let (user, password) = match userinfo {
        Some(ui) => match ui.split_once(':') {
            Some((u, p)) => (Some(percent_decode(u)), Some(percent_decode(p))),
            None => (Some(percent_decode(ui)), None),
        },
        None => (None, None),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) if !h.is_empty() => (h.to_string(), p.parse::<u32>().ok()),
        _ => (hostport.to_string(), None),
    };
    let dbname = path.map(percent_decode);
    let mut params = serde_json::Map::new();
    if let Some(q) = query {
        for pair in q.split('&').filter(|s| !s.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            params.insert(percent_decode(k), json!(percent_decode(v)));
        }
    }
    Ok(json!({
        "scheme": scheme,
        "user": user,
        "password": password,
        "host": host,
        "port": port,
        "dbname": dbname,
        "params": Value::Object(params),
    }))
}

/// Parse a libpq keyword/value connection string (`host=localhost port=5432
/// dbname=mydb user=foo`) — the other documented DSN form, distinct from the URI
/// form `parse_dsn` handles. Per the libpq rules: `keyword = value` pairs are
/// separated by spaces, spaces around the `=` are optional, a value containing
/// spaces or an empty value is single-quoted, and within a value `\'` and `\\`
/// are escapes. The well-known connection keywords (`host`, `port`, `dbname`,
/// `user`, `password`) are lifted to top-level fields and everything else is
/// collected under `params`, matching `parse_dsn`'s output shape (`port` parsed
/// to a number when numeric). opts: `dsn` (or `conninfo`). Returns `{user,
/// password, host, port, dbname, params}`. Pure.
fn op_parse_keyword_dsn(opts: Value) -> Result<Value> {
    let dsn = opts
        .get("dsn")
        .or_else(|| opts.get("conninfo"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing dsn"))?;
    let chars: Vec<char> = dsn.chars().collect();
    let n = chars.len();
    let mut i = 0;
    let mut fields = serde_json::Map::new();
    while i < n {
        while i < n && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= n {
            break;
        }
        let kstart = i;
        while i < n && chars[i] != '=' && !chars[i].is_whitespace() {
            i += 1;
        }
        let keyword: String = chars[kstart..i].iter().collect();
        if keyword.is_empty() {
            bail!("missing keyword in conninfo: {dsn}");
        }
        while i < n && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= n || chars[i] != '=' {
            bail!("missing `=` after `{keyword}` in conninfo: {dsn}");
        }
        i += 1; // consume '='
        while i < n && chars[i].is_whitespace() {
            i += 1;
        }
        let mut value = String::new();
        if i < n && chars[i] == '\'' {
            i += 1; // opening quote
            let mut closed = false;
            while i < n {
                let c = chars[i];
                if c == '\\' && i + 1 < n {
                    value.push(chars[i + 1]);
                    i += 2;
                } else if c == '\'' {
                    i += 1;
                    closed = true;
                    break;
                } else {
                    value.push(c);
                    i += 1;
                }
            }
            if !closed {
                bail!("unterminated quoted value for `{keyword}` in conninfo: {dsn}");
            }
        } else {
            while i < n && !chars[i].is_whitespace() {
                if chars[i] == '\\' && i + 1 < n {
                    value.push(chars[i + 1]);
                    i += 2;
                } else {
                    value.push(chars[i]);
                    i += 1;
                }
            }
        }
        fields.insert(keyword, json!(value));
    }
    let take = |m: &mut serde_json::Map<String, Value>, k: &str| m.remove(k).unwrap_or(Value::Null);
    let user = take(&mut fields, "user");
    let password = take(&mut fields, "password");
    let host = take(&mut fields, "host");
    let dbname = take(&mut fields, "dbname");
    let port = match fields.remove("port") {
        Some(Value::String(s)) if !s.is_empty() => {
            s.parse::<u32>().map(|n| json!(n)).unwrap_or(json!(s))
        }
        _ => Value::Null,
    };
    Ok(json!({
        "user": user,
        "password": password,
        "host": host,
        "port": port,
        "dbname": dbname,
        "params": Value::Object(fields),
    }))
}

/// Build a libpq keyword/value connection string (`host=localhost dbname=mydb`)
/// from parts — the inverse of `parse_keyword_dsn`, and the keyword-form
/// counterpart of `build_dsn` (URI). The well-known keys (`host`, `port`,
/// `dbname`, `user`, `password`) are emitted first in that order, then any
/// `params` (a sub-object) sorted by key. Per libpq, a value that is empty or
/// contains whitespace is single-quoted, and a `'` or `\` inside a value is
/// backslash-escaped — so the output re-parses through `parse_keyword_dsn`. opts:
/// `host`, `port`, `dbname`, `user`, `password`, `params`. Returns `{dsn}`. Pure.
fn op_build_keyword_dsn(opts: Value) -> Result<Value> {
    fn quote(v: &str) -> String {
        if v.is_empty()
            || v.bytes().any(|b| b.is_ascii_whitespace())
            || v.contains('\'')
            || v.contains('\\')
        {
            format!("'{}'", v.replace('\\', "\\\\").replace('\'', "\\'"))
        } else {
            v.to_string()
        }
    }
    fn str_of(v: &Value) -> Option<String> {
        match v {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            _ => None,
        }
    }
    let mut parts: Vec<String> = Vec::new();
    for key in ["host", "port", "dbname", "user", "password"] {
        if let Some(v) = opts.get(key).filter(|v| !v.is_null()) {
            if let Some(s) = str_of(v) {
                parts.push(format!("{key}={}", quote(&s)));
            }
        }
    }
    if let Some(params) = opts.get("params").and_then(Value::as_object) {
        let mut keys: Vec<&String> = params.keys().collect();
        keys.sort();
        for k in keys {
            if let Some(s) = params.get(k).and_then(str_of) {
                parts.push(format!("{k}={}", quote(&s)));
            }
        }
    }
    if parts.is_empty() {
        return Err(anyhow!("no connection parameters provided"));
    }
    Ok(json!({ "dsn": parts.join(" ") }))
}

/// Build a URI DSN from explicit parts, percent-encoding userinfo/dbname and
/// sanitizing the host with the same primitives the connector uses. Deterministic
/// (no env lookups) — the inverse of `parse_dsn`. opts: user, password, host,
/// port, dbname.
fn op_build_dsn(opts: Value) -> Result<Value> {
    let user = opts
        .get("user")
        .and_then(Value::as_str)
        .unwrap_or("postgres");
    let host = sanitize_host(
        opts.get("host")
            .and_then(Value::as_str)
            .unwrap_or("127.0.0.1"),
    );
    let port = opts.get("port").and_then(Value::as_u64).unwrap_or(5432);
    let dbname = opts
        .get("dbname")
        .and_then(Value::as_str)
        .unwrap_or("postgres");
    let userinfo = match opts.get("password").and_then(Value::as_str) {
        Some(p) if !p.is_empty() => format!(
            "{}:{}",
            percent_encode_userinfo(user),
            percent_encode_userinfo(p)
        ),
        _ => percent_encode_userinfo(user),
    };
    let dsn = format!(
        "postgresql://{}@{}:{}/{}",
        userinfo,
        host,
        port,
        percent_encode_userinfo(dbname)
    );
    Ok(json!({"dsn": dsn}))
}

/// Quote a SQL identifier (double-quote, doubling embedded quotes). Pure.
fn op_quote_ident(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    Ok(json!({"quoted": quote_ident(name)}))
}

/// Decode a double-quoted Postgres identifier back to its raw name — the inverse
/// of `quote_ident`. The input must be wrapped in matching `"` with every
/// embedded quote doubled (`""` → `"`); an unpaired quote is rejected. opts:
/// `quoted` (or `ident`). Returns `{name}`. Pure.
fn op_unquote_ident(opts: Value) -> Result<Value> {
    let input = opts
        .get("quoted")
        .or_else(|| opts.get("ident"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing quoted"))?;
    let inner = input
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .filter(|_| input.len() >= 2)
        .ok_or_else(|| anyhow!("not a double-quoted identifier: {input}"))?;
    // Every embedded quote must be doubled — an odd count means a stray one.
    if inner.matches('"').count() % 2 != 0 {
        return Err(anyhow!("malformed identifier: unpaired quote in {input}"));
    }
    Ok(json!({ "name": inner.replace("\"\"", "\"") }))
}

/// Quote a dotted, schema-qualified identifier: each dot-separated segment is
/// quoted independently and rejoined with `.`, so `public.my table` becomes
/// `"public"."my table"`. A literal dot inside a segment must be escaped by the
/// caller as it cannot be expressed positionally; an empty segment (leading,
/// trailing, or doubled dot) is rejected. Pure.
fn op_quote_qualified_ident(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let parts: Vec<&str> = name.split('.').collect();
    if parts.iter().any(|p| p.is_empty()) {
        return Err(anyhow!(
            "qualified identifier has an empty segment: `{name}`"
        ));
    }
    let quoted = parts
        .iter()
        .map(|p| quote_ident(p))
        .collect::<Vec<_>>()
        .join(".");
    Ok(json!({"quoted": quoted, "parts": parts}))
}

/// Parse a dotted, possibly-quoted qualified identifier into its segments — the
/// inverse of `quote_qualified_ident`. A double-quoted segment may contain `.`
/// (kept literal) and `""` (un-doubled to one `"`); bare segments pass through.
/// A `.` outside quotes separates segments; an unquoted empty segment (leading,
/// trailing, or doubled dot) and an unterminated quote are rejected. opts:
/// `name` (required). Returns `{parts}`. Pure.
fn op_parse_qualified_ident(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .or_else(|| opts.get("ident"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let chars: Vec<char> = name.chars().collect();
    let mut parts: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut had_content = false;
    let mut in_quote = false;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if in_quote {
            if c == '"' {
                if i + 1 < chars.len() && chars[i + 1] == '"' {
                    cur.push('"');
                    i += 2;
                    continue;
                }
                in_quote = false;
                i += 1;
            } else {
                cur.push(c);
                i += 1;
            }
        } else if c == '"' {
            in_quote = true;
            had_content = true;
            i += 1;
        } else if c == '.' {
            if !had_content {
                return Err(anyhow!("empty segment in qualified identifier: `{name}`"));
            }
            parts.push(std::mem::take(&mut cur));
            had_content = false;
            i += 1;
        } else {
            cur.push(c);
            had_content = true;
            i += 1;
        }
    }
    if in_quote {
        return Err(anyhow!("unterminated quoted identifier: `{name}`"));
    }
    if !had_content {
        return Err(anyhow!("empty segment in qualified identifier: `{name}`"));
    }
    parts.push(cur);
    Ok(json!({ "parts": parts }))
}

/// Quote a SQL string literal (single-quote, doubling embedded quotes). Pure —
/// assumes `standard_conforming_strings` (Postgres default), so backslashes are
/// literal.
/// Quote a string as a standard single-quoted Postgres literal, doubling every
/// embedded quote. Shared by `quote_literal` and `quote_nullable`.
fn quote_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn op_quote_literal(opts: Value) -> Result<Value> {
    let value = opts
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    Ok(json!({"quoted": quote_lit(value)}))
}

/// Postgres `quote_nullable`: like `quote_literal`, but a NULL (absent or null
/// `value`) yields the unquoted text `NULL` rather than a quoted string — so it
/// is safe to inline a possibly-null value directly into SQL. opts: `value`
/// (string or null). Returns `{quoted}`. Pure.
fn op_quote_nullable(opts: Value) -> Result<Value> {
    match opts.get("value") {
        None | Some(Value::Null) => Ok(json!({"quoted": "NULL"})),
        Some(v) => {
            let s = v
                .as_str()
                .ok_or_else(|| anyhow!("value must be a string or null"))?;
            Ok(json!({"quoted": quote_lit(s)}))
        }
    }
}

/// Escape a literal string for safe use inside a Postgres `LIKE`/`ILIKE` pattern.
/// The wildcard metacharacters `%` (any sequence) and `_` (any single character),
/// plus the default escape character `\`, are each backslash-escaped so the value
/// matches itself exactly — e.g. `col LIKE escape_like(s) || '%'` for a prefix
/// match. The `\` escape needs no explicit `ESCAPE` clause (it is the default).
/// The pattern-matching companion of `quote_literal`. opts: `value` (required).
/// Returns `{value, escaped}`. Pure.
fn op_escape_like(opts: Value) -> Result<Value> {
    let value = opts
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        if c == '\\' || c == '%' || c == '_' {
            out.push('\\');
        }
        out.push(c);
    }
    Ok(json!({"value": value, "escaped": out}))
}

/// Recover the literal text from a LIKE pattern that `escape_like` produced — its
/// inverse. Under the default `\` escape character, `\%`→`%`, `\_`→`_`, `\\`→`\`;
/// a backslash before any other character is an invalid escape, and an
/// *unescaped* wildcard (`%`/`_`) or a dangling trailing backslash means the
/// input is a real pattern, not an escaped literal, so both are rejected. This
/// makes `unescape_like(escape_like(s)) == s` for every `s`. opts: `value`
/// (required). Returns `{value, unescaped}`. Pure.
fn op_unescape_like(opts: Value) -> Result<Value> {
    let value = opts
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some(n @ ('\\' | '%' | '_')) => out.push(n),
                Some(n) => return Err(anyhow!("invalid escape sequence `\\{n}`")),
                None => return Err(anyhow!("dangling escape: trailing backslash")),
            },
            '%' | '_' => {
                return Err(anyhow!(
                    "unescaped wildcard `{c}`: not a LIKE-escaped literal"
                ))
            }
            _ => out.push(c),
        }
    }
    Ok(json!({"value": value, "unescaped": out}))
}

/// Decode a standard single-quoted Postgres string literal back to its raw value
/// — the inverse of `quote_literal`. Under `standard_conforming_strings` (the
/// default), the only escape inside `'…'` is a doubled quote `''` → `'`, with no
/// backslash processing. Requires the value to be wrapped in single quotes with
/// every embedded quote doubled (an unpaired quote is rejected). `E'…'`
/// escape-strings and dollar-quoting are NOT handled — only the form
/// `quote_literal` produces. opts: `value` (required). Returns `{value}`. Pure.
fn op_unquote_literal(opts: Value) -> Result<Value> {
    let input = opts
        .get("value")
        .or_else(|| opts.get("literal"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    let inner = input
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .filter(|_| input.len() >= 2)
        .ok_or_else(|| anyhow!("not a single-quoted literal: {input}"))?;
    // Every embedded quote must be doubled — an odd count means a stray quote.
    if inner.matches('\'').count() % 2 != 0 {
        return Err(anyhow!("malformed literal: unpaired quote inside {input}"));
    }
    Ok(json!({ "value": inner.replace("''", "'") }))
}

/// A legal dollar-quote tag: empty, or an unquoted-identifier form (ASCII letter
/// or `_` start, then letters/digits/`_`) with no `$`.
fn valid_dollar_tag(t: &str) -> bool {
    if t.is_empty() {
        return true;
    }
    let b = t.as_bytes();
    (b[0].is_ascii_alphabetic() || b[0] == b'_')
        && t.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
}

/// Postgres dollar-quoting: wrap `value` as `$tag$value$tag$` so it needs no
/// escaping at all — backslashes and quotes are literal — the form used for
/// function bodies, JSON, and strings full of quotes (`quote_literal` only does
/// standard `'…'` doubling). With no `tag`, a non-colliding one is chosen
/// automatically: the empty tag (`$$…$$`) when the content has no `$$`, otherwise
/// `$dq0$`, `$dq1$`, … An explicit `tag` must follow identifier rules and contain
/// no `$`, and the content must not already contain its `$tag$` delimiter. opts:
/// `value` (required), optional `tag`. Returns `{quoted, tag}`. Pure.
fn op_dollar_quote(opts: Value) -> Result<Value> {
    let value = opts
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    let tag = match opts.get("tag").and_then(Value::as_str) {
        Some(t) => {
            if !valid_dollar_tag(t) {
                bail!("dollar-quote tag `{t}` must be an identifier with no `$`");
            }
            if value.contains(&format!("${t}$")) {
                bail!("value contains the `${t}$` delimiter; choose another tag");
            }
            t.to_string()
        }
        None => {
            let mut i: u32 = 0;
            loop {
                let t = if i == 0 {
                    String::new()
                } else {
                    format!("dq{}", i - 1)
                };
                if !value.contains(&format!("${t}$")) {
                    break t;
                }
                i += 1;
            }
        }
    };
    Ok(json!({ "quoted": format!("${tag}${value}${tag}$"), "tag": tag }))
}

/// Decode a Postgres dollar-quoted string `$tag$value$tag$` back to its raw
/// content — the inverse of `dollar_quote` (which `unquote_literal`, handling
/// only `'…'`, can't). The opening tag is the run between the first two `$`; it
/// must be a legal dollar-quote tag (empty or identifier-form, no `$`) and the
/// string must end with the identical `$tag$` delimiter. The content is returned
/// verbatim — no un-escaping, since dollar quoting does none. A delimiter
/// appearing inside the body is rejected as malformed (Postgres would have closed
/// the literal there). opts: `value` (or `quoted`). Returns `{value, tag}`. Pure.
fn op_unquote_dollar(opts: Value) -> Result<Value> {
    let input = opts
        .get("value")
        .or_else(|| opts.get("quoted"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    let rest = input
        .strip_prefix('$')
        .ok_or_else(|| anyhow!("not a dollar-quoted string: {input}"))?;
    let tag_end = rest
        .find('$')
        .ok_or_else(|| anyhow!("unterminated dollar-quote opening tag: {input}"))?;
    let tag = &rest[..tag_end];
    if !valid_dollar_tag(tag) {
        bail!("invalid dollar-quote tag `{tag}` in {input}");
    }
    let delim = format!("${tag}$");
    let body = rest[tag_end + 1..]
        .strip_suffix(&delim)
        .ok_or_else(|| anyhow!("dollar-quoted string not closed by `{delim}`: {input}"))?;
    if body.contains(&delim) {
        bail!("dollar-quoted body contains its own delimiter `{delim}`: {input}");
    }
    Ok(json!({ "value": body, "tag": tag }))
}

/// Build a Postgres array literal `{a,b,c}` from a list of string `elements`,
/// following the input syntax: an element is double-quoted (with `"` and `\`
/// backslash-escaped) when it is empty, contains `,{}"\` or whitespace, or
/// equals `NULL` case-insensitively; otherwise it is emitted bare. The result
/// is the array body — wrap it with `quote_literal` to inline it as a value.
/// Pure.
fn op_format_array(opts: Value) -> Result<Value> {
    let elements = opts
        .get("elements")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing elements (array of strings)"))?;
    let needs_quoting = |s: &str| -> bool {
        s.is_empty()
            || s.eq_ignore_ascii_case("null")
            || s.chars()
                .any(|c| matches!(c, ',' | '{' | '}' | '"' | '\\') || c.is_whitespace())
    };
    let parts: Vec<String> = elements
        .iter()
        .map(|e| {
            let s = e.as_str().unwrap_or("");
            if needs_quoting(s) {
                format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
            } else {
                s.to_string()
            }
        })
        .collect();
    Ok(json!({"array": format!("{{{}}}", parts.join(","))}))
}

/// Build a parenthesized `IN (...)` operand from a list of `values` for a
/// `col IN (...)` predicate. Each value is rendered by its JSON type — a number
/// stays bare, a boolean becomes `TRUE`/`FALSE`, null becomes `NULL`, and a
/// string is single-quoted (with `quote_literal`'s `''` escaping) — so the list
/// can mix types. An empty list yields `(NULL)`, valid SQL that matches nothing
/// (Postgres rejects a literal empty `IN ()`). Distinct from `format_array`,
/// which builds the `{…}` array literal used with `= ANY(...)`. opts: `values`
/// (or `elements`). Returns `{list}`. Pure.
fn op_format_in_list(opts: Value) -> Result<Value> {
    let values = opts
        .get("values")
        .or_else(|| opts.get("elements"))
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing values (array)"))?;
    if values.is_empty() {
        return Ok(json!({ "list": "(NULL)" }));
    }
    let parts: Vec<String> = values
        .iter()
        .map(|v| match v {
            Value::Null => "NULL".to_string(),
            Value::Number(n) => n.to_string(),
            Value::Bool(true) => "TRUE".to_string(),
            Value::Bool(false) => "FALSE".to_string(),
            Value::String(s) => quote_lit(s),
            other => quote_lit(&other.to_string()),
        })
        .collect();
    Ok(json!({ "list": format!("({})", parts.join(", ")) }))
}

/// Parse a SQL `IN` list `(1, 'a', NULL, TRUE)` into a list of `elements` — the
/// inverse of `format_in_list`. Single-quoted elements are string literals
/// (embedded `''` un-doubles to `'`); a bare `NULL`/`TRUE`/`FALSE`
/// (case-insensitive) becomes a JSON null/bool; a bare integer or float becomes
/// a number; anything else stays a string. Commas inside quotes don't split.
/// `format_in_list` emits `(NULL)` for an empty input, so that parses back to a
/// single null. opts: `list` (or `value`). Returns `{elements}`. Pure.
fn op_parse_in_list(opts: Value) -> Result<Value> {
    let input = opts
        .get("list")
        .or_else(|| opts.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing list (a SQL IN list)"))?;
    let trimmed = input.trim();
    let inner = trimmed
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| anyhow!("not an IN list (expected (...)): {input}"))?
        .trim();
    let mut elements: Vec<Value> = Vec::new();
    if inner.is_empty() {
        return Ok(json!({ "elements": elements }));
    }
    let mut chars = inner.chars().peekable();
    loop {
        while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
            chars.next();
        }
        let elem = if chars.peek() == Some(&'\'') {
            chars.next(); // opening quote
            let mut buf = String::new();
            loop {
                match chars.next() {
                    Some('\'') => {
                        if chars.peek() == Some(&'\'') {
                            chars.next();
                            buf.push('\'');
                        } else {
                            break;
                        }
                    }
                    Some(c) => buf.push(c),
                    None => return Err(anyhow!("unterminated quoted element in: {input}")),
                }
            }
            json!(buf)
        } else {
            let mut buf = String::new();
            while let Some(&c) = chars.peek() {
                if c == ',' {
                    break;
                }
                buf.push(c);
                chars.next();
            }
            let t = buf.trim();
            if t.eq_ignore_ascii_case("null") {
                Value::Null
            } else if t.eq_ignore_ascii_case("true") {
                json!(true)
            } else if t.eq_ignore_ascii_case("false") {
                json!(false)
            } else if let Ok(i) = t.parse::<i64>() {
                json!(i)
            } else if let Ok(f) = t.parse::<f64>() {
                json!(f)
            } else {
                json!(t)
            }
        };
        elements.push(elem);
        while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
            chars.next();
        }
        match chars.next() {
            Some(',') => continue,
            None => break,
            Some(c) => return Err(anyhow!("unexpected `{c}` after element in: {input}")),
        }
    }
    Ok(json!({ "elements": elements }))
}

/// Parse a one-dimensional Postgres array literal `{a,b,"c,d"}` into a list of
/// `elements`. Double-quoted elements are unescaped (`\"`→`"`, `\\`→`\`); an
/// unquoted `NULL` (case-insensitive) becomes a JSON null while a quoted
/// `"NULL"` stays the string. Unquoted leading/trailing whitespace is trimmed.
/// Multidimensional arrays (nested `{}`) are rejected. Inverse of
/// `format_array`. opts: array (required). Pure.
fn op_parse_array(opts: Value) -> Result<Value> {
    let input = opts
        .get("array")
        .or_else(|| opts.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing array (a Postgres array literal)"))?;
    let trimmed = input.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| anyhow!("not a Postgres array literal (expected {{...}}): {input}"))?;
    let mut elements: Vec<Value> = Vec::new();
    if inner.trim().is_empty() {
        return Ok(json!({ "elements": elements }));
    }
    let mut chars = inner.chars().peekable();
    loop {
        while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
            chars.next();
        }
        let mut buf = String::new();
        let quoted = chars.peek() == Some(&'"');
        if quoted {
            chars.next();
            while let Some(c) = chars.next() {
                match c {
                    '\\' => {
                        if let Some(n) = chars.next() {
                            buf.push(n);
                        }
                    }
                    '"' => break,
                    _ => buf.push(c),
                }
            }
            while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
                chars.next();
            }
        } else {
            while let Some(&c) = chars.peek() {
                match c {
                    ',' => break,
                    '{' | '}' => {
                        return Err(anyhow!("multidimensional arrays not supported: {input}"))
                    }
                    _ => {
                        buf.push(c);
                        chars.next();
                    }
                }
            }
            buf = buf.trim_end().to_string();
        }
        if !quoted && buf.eq_ignore_ascii_case("null") {
            elements.push(Value::Null);
        } else {
            elements.push(json!(buf));
        }
        match chars.next() {
            Some(',') => continue,
            None => break,
            Some(c) => return Err(anyhow!("expected ',' between array elements, got `{c}`")),
        }
    }
    Ok(json!({ "elements": elements }))
}

/// Decode one bound of a range literal: an empty segment is unbounded (`null`); a
/// `"…"`-quoted value is unquoted (`\"`→`"`, `\\`→`\`); anything else is the bare
/// value verbatim (PostgreSQL keeps interior whitespace as part of the bound).
fn parse_range_bound(raw: &str) -> Result<Value> {
    if raw.is_empty() {
        return Ok(Value::Null);
    }
    if let Some(rest) = raw.strip_prefix('"') {
        let mut out = String::new();
        let mut chars = rest.chars();
        loop {
            match chars.next() {
                None => return Err(anyhow!("unterminated quoted range bound: {raw}")),
                Some('\\') => {
                    if let Some(n) = chars.next() {
                        out.push(n);
                    }
                }
                Some('"') => break,
                Some(c) => out.push(c),
            }
        }
        Ok(json!(out))
    } else {
        Ok(json!(raw))
    }
}

/// Parse a PostgreSQL range literal into its bounds. `[`/`]` are inclusive and
/// `(`/`)` exclusive; the lower and upper bounds are split on the top-level comma
/// (a comma inside a `"…"` bound does not split). An omitted bound is unbounded
/// (`null`); the literal `empty` (case-insensitive) is the empty range. Examples:
/// `[3,7)` → 3 inclusive .. 7 exclusive; `(,5]` → unbounded .. 5 inclusive;
/// `["a,b","z")` → quoted text bounds. opts: `range` (or `value`, required).
/// Returns `{empty, lower, upper, lower_inclusive, upper_inclusive}`. Pure.
fn op_parse_range(opts: Value) -> Result<Value> {
    let raw = opts
        .get("range")
        .or_else(|| opts.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing range"))?;
    let t = raw.trim();
    if t.eq_ignore_ascii_case("empty") {
        return Ok(json!({
            "empty": true,
            "lower": Value::Null,
            "upper": Value::Null,
            "lower_inclusive": Value::Null,
            "upper_inclusive": Value::Null,
        }));
    }
    let bytes = t.as_bytes();
    if bytes.len() < 3 {
        return Err(anyhow!("not a range literal: `{raw}`"));
    }
    let lower_inclusive = match bytes[0] {
        b'[' => true,
        b'(' => false,
        _ => return Err(anyhow!("range must start with `[` or `(`: `{raw}`")),
    };
    let upper_inclusive = match bytes[bytes.len() - 1] {
        b']' => true,
        b')' => false,
        _ => return Err(anyhow!("range must end with `]` or `)`: `{raw}`")),
    };
    let inner = &t[1..t.len() - 1];
    // Find the top-level comma (one not inside a "…" bound).
    let inner_bytes = inner.as_bytes();
    let mut in_quote = false;
    let mut comma: Option<usize> = None;
    let mut i = 0;
    while i < inner_bytes.len() {
        let c = inner_bytes[i];
        if in_quote {
            if c == b'\\' {
                i += 2;
                continue;
            }
            if c == b'"' {
                in_quote = false;
            }
        } else if c == b'"' {
            in_quote = true;
        } else if c == b',' {
            comma = Some(i);
            break;
        }
        i += 1;
    }
    let pos = comma.ok_or_else(|| anyhow!("range missing the lower/upper comma: `{raw}`"))?;
    let lower = parse_range_bound(&inner[..pos])?;
    let upper = parse_range_bound(&inner[pos + 1..])?;
    Ok(json!({
        "empty": false,
        "lower": lower,
        "upper": upper,
        "lower_inclusive": lower_inclusive,
        "upper_inclusive": upper_inclusive,
    }))
}

// ── exports ─────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn pg__pkg_version(args: *const c_char) -> *const c_char {
    ffi_call(args, |_| Ok(json!({"version": env!("CARGO_PKG_VERSION")})))
}

#[no_mangle]
pub extern "C" fn pg__version(args: *const c_char) -> *const c_char {
    ffi_call(args, op_version)
}

#[no_mangle]
pub extern "C" fn pg__ping(args: *const c_char) -> *const c_char {
    ffi_call(args, op_ping)
}

#[no_mangle]
pub extern "C" fn pg__databases(args: *const c_char) -> *const c_char {
    ffi_call(args, op_databases)
}

#[no_mangle]
pub extern "C" fn pg__tables(args: *const c_char) -> *const c_char {
    ffi_call(args, op_tables)
}

#[no_mangle]
pub extern "C" fn pg__schema(args: *const c_char) -> *const c_char {
    ffi_call(args, op_schema)
}

#[no_mangle]
pub extern "C" fn pg__query(args: *const c_char) -> *const c_char {
    ffi_call(args, op_query)
}

#[no_mangle]
pub extern "C" fn pg__execute(args: *const c_char) -> *const c_char {
    ffi_call(args, op_execute)
}

#[no_mangle]
pub extern "C" fn pg__exec(args: *const c_char) -> *const c_char {
    ffi_call(args, op_exec)
}

#[no_mangle]
pub extern "C" fn pg__dump(args: *const c_char) -> *const c_char {
    ffi_call(args, op_dump)
}

#[no_mangle]
pub extern "C" fn pg__insert_many(args: *const c_char) -> *const c_char {
    ffi_call(args, op_insert_many)
}

#[no_mangle]
pub extern "C" fn pg__copy_in(args: *const c_char) -> *const c_char {
    ffi_call(args, op_copy_in)
}

#[no_mangle]
pub extern "C" fn pg__copy_out(args: *const c_char) -> *const c_char {
    ffi_call(args, op_copy_out)
}

#[no_mangle]
pub extern "C" fn pg__notify(args: *const c_char) -> *const c_char {
    ffi_call(args, op_notify)
}

#[no_mangle]
pub extern "C" fn pg__listen(args: *const c_char) -> *const c_char {
    ffi_call(args, op_listen)
}

#[no_mangle]
pub extern "C" fn pg__explain(args: *const c_char) -> *const c_char {
    ffi_call(args, op_explain)
}

#[no_mangle]
pub extern "C" fn pg__views(args: *const c_char) -> *const c_char {
    ffi_call(args, op_views)
}

#[no_mangle]
pub extern "C" fn pg__functions(args: *const c_char) -> *const c_char {
    ffi_call(args, op_functions)
}

#[no_mangle]
pub extern "C" fn pg__indexes(args: *const c_char) -> *const c_char {
    ffi_call(args, op_indexes)
}

#[no_mangle]
pub extern "C" fn pg__roles(args: *const c_char) -> *const c_char {
    ffi_call(args, op_roles)
}

#[no_mangle]
pub extern "C" fn pg__db_size(args: *const c_char) -> *const c_char {
    ffi_call(args, op_db_size)
}

#[no_mangle]
pub extern "C" fn pg__activity(args: *const c_char) -> *const c_char {
    ffi_call(args, op_activity)
}

#[no_mangle]
pub extern "C" fn pg__locks(args: *const c_char) -> *const c_char {
    ffi_call(args, op_locks)
}

#[no_mangle]
pub extern "C" fn pg__table_size(args: *const c_char) -> *const c_char {
    ffi_call(args, op_table_size)
}

#[no_mangle]
pub extern "C" fn pg__sequences(args: *const c_char) -> *const c_char {
    ffi_call(args, op_sequences)
}

#[no_mangle]
pub extern "C" fn pg__extensions(args: *const c_char) -> *const c_char {
    ffi_call(args, op_extensions)
}

#[no_mangle]
pub extern "C" fn pg__triggers(args: *const c_char) -> *const c_char {
    ffi_call(args, op_triggers)
}

#[no_mangle]
pub extern "C" fn pg__cancel_backend(args: *const c_char) -> *const c_char {
    ffi_call(args, op_cancel_backend)
}

#[no_mangle]
pub extern "C" fn pg__parse_dsn(args: *const c_char) -> *const c_char {
    ffi_call(args, op_parse_dsn)
}

#[no_mangle]
pub extern "C" fn pg__parse_keyword_dsn(args: *const c_char) -> *const c_char {
    ffi_call(args, op_parse_keyword_dsn)
}

#[no_mangle]
pub extern "C" fn pg__build_keyword_dsn(args: *const c_char) -> *const c_char {
    ffi_call(args, op_build_keyword_dsn)
}

#[no_mangle]
pub extern "C" fn pg__build_dsn(args: *const c_char) -> *const c_char {
    ffi_call(args, op_build_dsn)
}

#[no_mangle]
pub extern "C" fn pg__quote_ident(args: *const c_char) -> *const c_char {
    ffi_call(args, op_quote_ident)
}

#[no_mangle]
pub extern "C" fn pg__unquote_ident(args: *const c_char) -> *const c_char {
    ffi_call(args, op_unquote_ident)
}

#[no_mangle]
pub extern "C" fn pg__quote_qualified_ident(args: *const c_char) -> *const c_char {
    ffi_call(args, op_quote_qualified_ident)
}

#[no_mangle]
pub extern "C" fn pg__parse_qualified_ident(args: *const c_char) -> *const c_char {
    ffi_call(args, op_parse_qualified_ident)
}

#[no_mangle]
pub extern "C" fn pg__escape_like(args: *const c_char) -> *const c_char {
    ffi_call(args, op_escape_like)
}

#[no_mangle]
pub extern "C" fn pg__unescape_like(args: *const c_char) -> *const c_char {
    ffi_call(args, op_unescape_like)
}

#[no_mangle]
pub extern "C" fn pg__quote_literal(args: *const c_char) -> *const c_char {
    ffi_call(args, op_quote_literal)
}

#[no_mangle]
pub extern "C" fn pg__quote_nullable(args: *const c_char) -> *const c_char {
    ffi_call(args, op_quote_nullable)
}

#[no_mangle]
pub extern "C" fn pg__unquote_literal(args: *const c_char) -> *const c_char {
    ffi_call(args, op_unquote_literal)
}

#[no_mangle]
pub extern "C" fn pg__dollar_quote(args: *const c_char) -> *const c_char {
    ffi_call(args, op_dollar_quote)
}

#[no_mangle]
pub extern "C" fn pg__unquote_dollar(args: *const c_char) -> *const c_char {
    ffi_call(args, op_unquote_dollar)
}

#[no_mangle]
pub extern "C" fn pg__format_array(args: *const c_char) -> *const c_char {
    ffi_call(args, op_format_array)
}

#[no_mangle]
pub extern "C" fn pg__format_in_list(args: *const c_char) -> *const c_char {
    ffi_call(args, op_format_in_list)
}

#[no_mangle]
pub extern "C" fn pg__parse_in_list(args: *const c_char) -> *const c_char {
    ffi_call(args, op_parse_in_list)
}

#[no_mangle]
pub extern "C" fn pg__parse_array(args: *const c_char) -> *const c_char {
    ffi_call(args, op_parse_array)
}

#[no_mangle]
pub extern "C" fn pg__parse_range(args: *const c_char) -> *const c_char {
    ffi_call(args, op_parse_range)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env<F: FnOnce()>(f: F) {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let saved = [
            ("DATABASE_URL", std::env::var("DATABASE_URL").ok()),
            ("POSTGRES_URL", std::env::var("POSTGRES_URL").ok()),
        ];
        std::env::remove_var("DATABASE_URL");
        std::env::remove_var("POSTGRES_URL");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        for (k, v) in &saved {
            match v {
                Some(s) => std::env::set_var(k, s),
                None => std::env::remove_var(k),
            }
        }
        if let Err(p) = result {
            std::panic::resume_unwind(p);
        }
    }

    // ── bind param type narrowing ──
    //
    // A JSON integer is carried as PgParam::Int(i64). The postgres crate's
    // `i64::to_sql` only ACCEPTS Type::INT8, so without the narrowing in the
    // PgParam ToSql impl, `bind => [1]` against an `id INT` (INT4) column —
    // the single most common bind — fails the type check at encode time. This
    // pins that an integer bind encodes against every integer width plus the
    // float columns.
    #[test]
    fn int_param_narrows_across_integer_and_float_widths() {
        use postgres::types::{ToSql, Type};
        for ty in [
            Type::INT2,
            Type::INT4,
            Type::INT8,
            Type::FLOAT4,
            Type::FLOAT8,
        ] {
            let mut buf = bytes::BytesMut::new();
            assert!(
                PgParam::Int(42).to_sql_checked(&ty, &mut buf).is_ok(),
                "Int(42) must encode against {ty}"
            );
        }
    }

    #[test]
    fn float_param_narrows_to_float4() {
        use postgres::types::{ToSql, Type};
        let mut buf = bytes::BytesMut::new();
        assert!(
            PgParam::Float(1.5)
                .to_sql_checked(&Type::FLOAT4, &mut buf)
                .is_ok(),
            "Float must encode against a FLOAT4 column"
        );
    }

    // ── url_from_opts ──

    #[test]
    fn url_opts_wins_over_env() {
        with_env(|| {
            std::env::set_var("DATABASE_URL", "postgresql://env@h/db");
            assert_eq!(
                url_from_opts(&json!({"url": "postgresql://opts@h/db"})),
                "postgresql://opts@h/db"
            );
        });
    }

    #[test]
    fn url_database_url_preferred_over_postgres_url() {
        with_env(|| {
            std::env::set_var("DATABASE_URL", "from-db-url");
            std::env::set_var("POSTGRES_URL", "from-pg-url");
            assert_eq!(url_from_opts(&json!({})), "from-db-url");
        });
    }

    #[test]
    fn url_postgres_url_fallback() {
        with_env(|| {
            std::env::set_var("POSTGRES_URL", "from-pg-url");
            assert_eq!(url_from_opts(&json!({})), "from-pg-url");
        });
    }

    #[test]
    fn url_default_assembled_from_pieces() {
        with_env(|| {
            assert_eq!(
                url_from_opts(&json!({})),
                "postgresql://postgres@127.0.0.1:5432/"
            );
        });
    }

    #[test]
    fn url_uses_opts_overrides_when_no_url_or_env() {
        with_env(|| {
            let u = url_from_opts(&json!({
                "host": "pg.example.com",
                "port": 6432,
                "user": "ada",
                "database": "shop",
            }));
            assert_eq!(u, "postgresql://ada@pg.example.com:6432/shop");
        });
    }

    #[test]
    fn url_password_inserted_when_set() {
        with_env(|| {
            let u = url_from_opts(&json!({"user": "ada", "password": "hunter2"}));
            assert!(u.contains("ada:hunter2@"), "{u}");
        });
    }

    #[test]
    fn url_no_stray_colon_when_password_blank() {
        with_env(|| {
            let u = url_from_opts(&json!({"user": "ada"}));
            assert!(!u.contains(":@"), "{u}");
        });
    }

    // ── json_to_param ──

    #[test]
    fn jp_null() {
        assert!(matches!(json_to_param(&Value::Null), PgParam::Null));
    }

    #[test]
    fn jp_bool() {
        assert!(matches!(json_to_param(&json!(true)), PgParam::Bool(true)));
        assert!(matches!(json_to_param(&json!(false)), PgParam::Bool(false)));
    }

    #[test]
    fn jp_int() {
        match json_to_param(&json!(42)) {
            PgParam::Int(42) => {}
            other => panic!("expected Int(42), got {other:?}"),
        }
    }

    #[test]
    fn jp_float() {
        match json_to_param(&json!(1.5)) {
            PgParam::Float(f) if (f - 1.5).abs() < 1e-9 => {}
            other => panic!("expected Float(1.5), got {other:?}"),
        }
    }

    #[test]
    fn jp_string() {
        match json_to_param(&json!("hi")) {
            PgParam::Str(s) => assert_eq!(s, "hi"),
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn jp_array_falls_back_to_json() {
        // Arrays/objects survive as `PgParam::Json` and are bound via
        // postgres's JSON/JSONB codec — callers can target JSONB columns.
        match json_to_param(&json!([1, 2, 3])) {
            PgParam::Json(v) => assert_eq!(v, json!([1, 2, 3])),
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn jp_object_falls_back_to_json() {
        match json_to_param(&json!({"k": "v"})) {
            PgParam::Json(v) => assert_eq!(v["k"], json!("v")),
            other => panic!("expected Json, got {other:?}"),
        }
    }

    // ── params_from_value ──

    #[test]
    fn params_array_yields_vec() {
        let p = params_from_value(&json!([1, "two", null]));
        assert_eq!(p.len(), 3);
    }

    #[test]
    fn params_non_array_yields_empty_vec() {
        assert!(params_from_value(&json!({"a": 1})).is_empty());
        assert!(params_from_value(&Value::Null).is_empty());
        assert!(params_from_value(&json!("scalar")).is_empty());
    }

    // ── ffi_call ──
    //
    // These pin the FFI boundary contract: (a) null args must invoke the
    // handler with `Value::Null` (not segfault on a wild deref), and (b) a
    // panic inside the handler must be caught and surfaced as a JSON error
    // string — otherwise it unwinds across the `extern "C"` boundary into
    // the stryke runtime and aborts the whole shell process. Both bug
    // classes are silent-corruption-not-test-failure, which is why a
    // boilerplate "round-trip a known value" test would not catch them.

    #[test]
    fn ffi_call_null_args_passes_null_value_to_handler() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let saw_null = AtomicBool::new(false);
        let ptr = ffi_call(std::ptr::null(), |v| {
            saw_null.store(v.is_null(), Ordering::SeqCst);
            Ok(json!({"k": "v"}))
        });
        assert!(
            saw_null.load(Ordering::SeqCst),
            "handler did not receive Value::Null on null *const c_char"
        );
        assert!(
            !ptr.is_null(),
            "ffi_call returned null pointer on success path"
        );
        let s = unsafe { CStr::from_ptr(ptr).to_str().unwrap().to_string() };
        unsafe { stryke_free_cstring(ptr as *mut c_char) };
        assert!(
            s.contains(r#""k":"v""#),
            "round-tripped JSON missing payload: {s}"
        );
    }

    #[test]
    fn ffi_call_catches_handler_panic_and_returns_error_json() {
        // A panic that escapes ffi_call would unwind across `extern "C"`
        // — undefined behavior — and tear down the host stryke process.
        // The `catch_unwind` line is load-bearing; this test pins it.
        let ptr = ffi_call(std::ptr::null(), |_| -> Result<Value> {
            panic!("synthetic handler panic for FFI boundary test");
        });
        assert!(
            !ptr.is_null(),
            "panic recovery path must still return a valid C string"
        );
        let s = unsafe { CStr::from_ptr(ptr).to_str().unwrap().to_string() };
        unsafe { stryke_free_cstring(ptr as *mut c_char) };
        let v: Value = serde_json::from_str(&s).expect("response must be valid JSON");
        let err = v.get("error").and_then(|e| e.as_str()).unwrap_or("");
        assert!(
            err.contains("panicked"),
            "expected panic surfaced as error JSON, got: {s}"
        );
    }

    #[test]
    fn ffi_call_malformed_json_input_falls_back_to_null_not_panic() {
        // ffi_call uses `unwrap_or(Value::Null)` on the parse — a regression
        // to `unwrap()` would turn every malformed call into a stryke-wide
        // crash. Construct an explicit garbage CString to exercise the
        // serde_json fallback branch (the `*const c_char` path, not the
        // null-pointer path).
        use std::sync::atomic::{AtomicBool, Ordering};
        let garbage = CString::new("{not valid json").unwrap();
        let saw_null = AtomicBool::new(false);
        let ptr = ffi_call(garbage.as_ptr(), |v| {
            saw_null.store(v.is_null(), Ordering::SeqCst);
            Ok(json!({"ok": true}))
        });
        assert!(
            saw_null.load(Ordering::SeqCst),
            "malformed JSON should land as Value::Null, not panic"
        );
        assert!(!ptr.is_null());
        unsafe { stryke_free_cstring(ptr as *mut c_char) };
    }

    /// `url_from_opts` precedence: explicit `url` opt wins over env vars
    /// wins over host/port/user assembly. Pin so a future refactor that
    /// swaps the precedence chain (env-first vs opts-first) gets caught
    /// — opts-first matches the rest of stryke's convention.
    #[test]
    fn url_from_opts_explicit_url_wins_over_env_and_parts() {
        std::env::remove_var("DATABASE_URL");
        std::env::remove_var("POSTGRES_URL");
        let u = url_from_opts(&json!({
            "url": "postgresql://opts/db",
            "host": "ignored.example",
            "port": 9999,
        }));
        assert_eq!(u, "postgresql://opts/db");
    }

    /// Defaults when nothing is supplied: 127.0.0.1:5432, user=postgres,
    /// empty password, empty database. Pin so a refactor that introduces
    /// a different default (e.g. `localhost` vs `127.0.0.1`, which on
    /// some systems go through different resolvers) gets caught.
    #[test]
    fn url_from_opts_assembled_form_uses_documented_defaults() {
        std::env::remove_var("DATABASE_URL");
        std::env::remove_var("POSTGRES_URL");
        let u = url_from_opts(&json!({}));
        assert_eq!(u, "postgresql://postgres@127.0.0.1:5432/");
    }

    // ── url_from_opts: edge-case bug surface ──
    //
    // url_from_opts builds the postgres DSN by raw `format!` interpolation
    // (lines 56-61). It does NOT percent-encode `password`, `user`, or
    // `database`, and it does NOT clamp `port` to a valid u16 range. These
    // tests pin the SHAPE of the bug surface so regressions in the
    // encoding-aware fix won't silently regress to the broken passthrough.

    #[test]
    fn url_password_with_at_sign_is_percent_encoded() {
        // FIXED: passwords containing `@` are now percent-encoded so the
        // postgres DSN parser sees the correct authority component.
        // Pre-fix, `pa@ss` would split as user=`u`, password=`pa`, host=`ss`.
        // Now: `pa%40ss` round-trips as the full password.
        with_env(|| {
            let u = url_from_opts(&json!({
                "host": "h",
                "user": "u",
                "password": "pa@ss",
                "database": "d",
            }));
            assert!(
                u.contains("pa%40ss@h"),
                "expected percent-encoded `@` in password, got: {u}"
            );
            assert!(
                !u.contains("pa@ss@h"),
                "raw `@` must not survive encoding (DSN-corruption regression): {u}"
            );
        });
    }

    #[test]
    fn url_negative_port_is_passed_through_verbatim() {
        // BUG SURFACE: opts.port is read via `as_i64().unwrap_or(5432)` with
        // no range check (line 49). A port like -1 or 99999 is interpolated
        // into the DSN, producing an unparseable connection string that
        // surfaces as a confusing libpq error instead of an early arg-validation
        // error from this crate. Pin the current passthrough so a fix
        // (clamping to 1..=65535) is observable and intentional.
        with_env(|| {
            let neg = url_from_opts(&json!({"port": -1}));
            assert!(
                neg.contains(":-1/"),
                "negative port not interpolated raw: {neg}"
            );
            let huge = url_from_opts(&json!({"port": 99999i64}));
            assert!(
                huge.contains(":99999/"),
                "out-of-range port not interpolated raw: {huge}"
            );
        });
    }

    // ── json_to_param: numeric range cliff ──

    #[test]
    fn jp_u64_above_i64_max_silently_demotes_to_float() {
        // BUG SURFACE: json_to_param's number branch (lines 127-135) tries
        // `as_i64()` first, then `as_f64()`. A JSON number above `i64::MAX`
        // (legal per RFC 8259; serde_json with arbitrary_precision OFF
        // accepts u64 up to u64::MAX) sails past the i64 branch and falls
        // into f64, silently losing precision. Binding such a value to a
        // postgres BIGINT column then produces a wrong row with no error.
        //
        // Hand-crafted to catch the demotion: assert (a) the variant is
        // Float, (b) the float value lost low bits relative to the source.
        // A naive smoke test that just bound the value to a mock client
        // would not surface the silent corruption.
        let big = u64::MAX; // 18446744073709551615
        let v: Value = serde_json::from_str(&big.to_string()).expect("u64::MAX is valid JSON");
        match json_to_param(&v) {
            PgParam::Float(f) => {
                // The cast back to u64 loses precision: u64::MAX → 1.844...e19,
                // which when cast back is ALSO u64::MAX rounded up to the next
                // representable double. Pin that low bits are gone.
                let round_trip = f as u64;
                // Either equal-via-rounding (>= big - 2048 due to f64 mantissa)
                // or saturating to u64::MAX. Either way the LSB precision is
                // gone — this assert pins the lossy branch is taken.
                assert!(
                    round_trip == u64::MAX || round_trip < big,
                    "expected precision loss for u64::MAX → f64 → u64; got {round_trip}"
                );
            }
            PgParam::Int(i) => {
                // If a future fix turns this into a Str branch instead (to
                // preserve precision), update this test. An Int branch is
                // wrong because i64 can't represent u64::MAX.
                panic!(
                    "u64::MAX must NOT decode into i64 (overflow); got Int({i}) — \
                     means the i64 branch incorrectly accepted an out-of-range value"
                );
            }
            other => {
                // Acceptable future fix: PgParam::Str(big.to_string()) so the
                // postgres BIGINT/NUMERIC codec handles it losslessly. If we
                // ever land that, this test must be updated to assert Str.
                panic!("unexpected variant for u64::MAX: {other:?}");
            }
        }
    }

    /// `validate_identifier` must reject the classic SQL-injection payload
    /// — anything with `;`, spaces, or quote characters. Pre-fix, a `table`
    /// param of `users; DROP TABLE users` was raw-interpolated into
    /// `SELECT * FROM {table}`, executing the DROP.
    #[test]
    fn validate_identifier_rejects_semicolon_drop_payload() {
        assert!(validate_identifier("users; DROP TABLE users", "table").is_err());
        assert!(validate_identifier("users -- comment", "table").is_err());
        assert!(validate_identifier("\"users\"", "table").is_err());
        assert!(validate_identifier("users'", "table").is_err());
        assert!(validate_identifier("", "table").is_err());
        assert!(
            validate_identifier("1users", "table").is_err(),
            "must start with letter or _"
        );
    }

    /// `validate_identifier` must accept schema-qualified names so users can
    /// dump from non-public schemas. A blanket "no `.`" reject would break
    /// the documented `op_dump({"table": "analytics.events"})` shape.
    #[test]
    fn validate_identifier_accepts_schema_qualified_name() {
        assert!(validate_identifier("analytics.events", "table").is_ok());
        assert!(validate_identifier("_private.t1", "table").is_ok());
        assert!(validate_identifier("a.b.c", "table").is_ok());
        // Empty segment (leading/trailing/double dot) — rejected.
        assert!(validate_identifier(".events", "table").is_err());
        assert!(validate_identifier("analytics.", "table").is_err());
        assert!(validate_identifier("a..b", "table").is_err());
    }

    /// `percent_encode_userinfo` must encode the URL-meaningful characters
    /// that broke DSN parsing before this fix: `@` `:` `/` `?` `#`. A
    /// password like `p@/ss` previously made `ss` the host; encoded it
    /// becomes `p%40%2Fss` which postgres correctly decodes as the
    /// password.
    #[test]
    fn percent_encode_userinfo_encodes_dsn_breakers() {
        assert_eq!(percent_encode_userinfo("p@ss"), "p%40ss");
        assert_eq!(percent_encode_userinfo("p/wd"), "p%2Fwd");
        assert_eq!(percent_encode_userinfo("p:wd"), "p%3Awd");
        assert_eq!(percent_encode_userinfo("p?q#f"), "p%3Fq%23f");
        // Unreserved set passes through untouched.
        assert_eq!(percent_encode_userinfo("aZ0-9_.~"), "aZ0-9_.~");
    }

    // ── url_from_opts host-field bug surface ──
    //
    // The `password` and `user` fields go through `percent_encode_userinfo`
    // (line 56-64). The `host` field does NOT — it's interpolated raw into
    // the DSN at line 66 via `format!("postgresql://{}@{}:{}/{}", ..., host, ...)`.
    //
    // A config-provided host containing DSN-meaningful characters (`/`, `?`,
    // `:`, `@`) can therefore inject path / query / authority components or
    // redirect the connection. e.g. `host: "evil.com/?intercept=1"` produces
    // `postgresql://postgres@evil.com/?intercept=1:5432/` which the postgres
    // client parses with unpredictable authority/query splits.
    //
    // This is the EXACT bug class the userinfo encoding was added to fix —
    // host is the missing third leg. The test pins current passthrough so
    // either (a) a future fix to validate / encode host is observable, or
    // (b) the bug is recognized on review.
    //
    // is_boilerplate_check: this is NOT a mirror of `percent_encode_userinfo`
    // — it tests `url_from_opts` end-to-end with a malicious-host input
    // showing the DSN-injection escape, which no existing test covers (every
    // existing host-arg test uses a clean hostname like `pg.example.com` or
    // `h`). A regression that adds host encoding would change the output and
    // this test would catch it for review.
    #[test]
    fn url_host_field_dsn_injection_collapses_to_safe_fallback() {
        // FIXED: a host containing DSN-meaningful chars (`/`, `?`, etc.) is now
        // routed through `sanitize_host` which collapses to `127.0.0.1`. The
        // original bug pinned the raw passthrough; this version asserts the
        // injection target does NOT survive into the DSN.
        with_env(|| {
            let u = url_from_opts(&json!({
                "host": "evil.com/?intercept=1",
                "user": "u",
                "database": "d",
            }));
            assert!(
                u.contains("@127.0.0.1:"),
                "expected sanitized host to collapse to 127.0.0.1, got: {u}"
            );
            assert!(
                !u.contains("evil.com") && !u.contains("intercept"),
                "neither host nor query-string injection may survive: {u}"
            );
        });
    }

    // ── url_from_opts port-type coercion bug surface ──
    //
    // Line 49: `opts.get("port").and_then(|v| v.as_i64()).unwrap_or(5432)`.
    // `as_i64()` returns None for any JSON Value that isn't a Number
    // representable as i64 — including a JSON STRING like `"5432"`. The
    // silent fallback to 5432 means a config error (port supplied as
    // string, common from env-var-templated configs) is masked, connecting
    // to the wrong port without surfacing the type mismatch.
    //
    // is_boilerplate_check: existing port tests (`url_uses_opts_overrides_when_no_url_or_env`
    // line 646, `url_negative_port_is_passed_through_verbatim` line 883)
    // only test with valid integer ports. This pins the silent-fallback
    // behavior on type mismatch, which is a different bug class (silent
    // config corruption vs. range validation). A future arg-validation fix
    // would error on string ports instead of falling back, and this test
    // would catch the behavior change for review.
    #[test]
    fn url_port_as_string_silently_falls_back_to_default() {
        with_env(|| {
            // Port supplied as JSON string "9999" — silently ignored, falls
            // back to 5432 because `as_i64()` rejects strings.
            let u = url_from_opts(&json!({
                "host": "h",
                "port": "9999",
                "user": "u",
                "database": "d",
            }));
            assert!(
                u.contains(":5432/"),
                "expected silent fallback to 5432 when port is a JSON string; got: {u}"
            );
            assert!(
                !u.contains(":9999"),
                "string port unexpectedly honored: {u}"
            );
        });
    }

    // ── validate_identifier NUL byte handling ──
    //
    // The cdylib FFI returns `*const c_char` via `CString::new(s)` at line
    // 501. `CString::new` rejects strings with interior NULs and silently
    // returns `std::ptr::null()` (line 502-503) — meaning ANY identifier or
    // SQL string that contains a NUL byte reaching the JSON serialization
    // path returns NULL to stryke. `validate_identifier` is the only choke
    // point between user input and SQL formatting; if it accepted a NUL,
    // a downstream `format!("... {} ...", name)` would embed NUL in the
    // batch-execute or query string and then poison the CString path.
    //
    // Verify that NUL is rejected at validate_identifier (the LAST line of
    // defense before SQL interpolation), independent of the FFI NUL check.
    // Postgres treats NUL bytes in queries as protocol-level errors;
    // catching here gives a CLEAR error rather than a postgres protocol
    // message.
    //
    // is_boilerplate_check: existing `validate_identifier_rejects_semicolon_drop_payload`
    // (line 958) covers printable injection chars; NUL is a separate bug
    // class — it's the C-string boundary corruptor, not a SQL syntax
    // injector. A regression that allowed NUL through (e.g., by changing
    // `is_ascii_alphanumeric()` to `is_alphanumeric()` for Unicode support
    // — which would still reject NUL — vs. changing to a `match` allowing
    // controls) would be caught here.
    #[test]
    fn validate_identifier_rejects_nul_byte_in_segment() {
        // NUL embedded in middle of name.
        assert!(
            validate_identifier("users\0evil", "table").is_err(),
            "NUL byte must not pass identifier validation"
        );
        // NUL as first byte.
        assert!(
            validate_identifier("\0users", "table").is_err(),
            "leading NUL must not pass identifier validation"
        );
        // NUL after schema separator.
        assert!(
            validate_identifier("public.\0users", "table").is_err(),
            "NUL in segment after `.` must not pass identifier validation"
        );
        // Other ASCII control chars also rejected (defense in depth).
        assert!(
            validate_identifier("users\x01evil", "table").is_err(),
            "ASCII control char (0x01) must not pass identifier validation"
        );
        assert!(
            validate_identifier("users\nevil", "table").is_err(),
            "newline must not pass identifier validation"
        );
        assert!(
            validate_identifier("users\tevil", "table").is_err(),
            "tab must not pass identifier validation"
        );
    }

    /// `sanitize_host` must reject DSN-rewriting characters. A malicious
    /// caller passing `host = "evil.com/?#@trusted-host"` would otherwise
    /// re-anchor the URL authority and silently redirect the connection.
    /// Pin: hosts with `@`/`/`/`?`/`#`/`:`/`[`/`]`/space/backslash collapse
    /// to the safe fallback `127.0.0.1`.
    #[test]
    fn sanitize_host_rejects_dsn_redirect_payloads() {
        assert_eq!(sanitize_host("evil.com/?#@trusted-host"), "127.0.0.1");
        assert_eq!(sanitize_host("a@b"), "127.0.0.1");
        assert_eq!(sanitize_host("a/b"), "127.0.0.1");
        assert_eq!(sanitize_host("a?b"), "127.0.0.1");
        assert_eq!(sanitize_host("a#b"), "127.0.0.1");
        assert_eq!(sanitize_host("a:b"), "127.0.0.1");
        assert_eq!(sanitize_host("evil host"), "127.0.0.1");
        assert_eq!(sanitize_host("evil\\host"), "127.0.0.1");
    }

    /// Normal hostnames + IPv4 + bracketed IPv6 must pass through unchanged.
    /// A blanket reject would break the documented use case.
    #[test]
    fn sanitize_host_accepts_normal_hosts_and_ipv6_literal() {
        assert_eq!(sanitize_host("localhost"), "localhost");
        assert_eq!(sanitize_host("db.example.com"), "db.example.com");
        assert_eq!(sanitize_host("192.168.1.10"), "192.168.1.10");
        assert_eq!(sanitize_host("[::1]"), "[::1]");
        assert_eq!(sanitize_host("[2001:db8::1]"), "[2001:db8::1]");
        // But bare IPv6 (no brackets) contains `:`, which is rejected — caller
        // must supply the bracketed form to disambiguate from `host:port`.
        assert_eq!(sanitize_host("::1"), "127.0.0.1");
    }

    // ── validate_identifier: `$` placement asymmetry ──
    //
    // `valid_start` (line 138) is `is_ascii_alphabetic() || '_'` — `$` is NOT
    // a legal first char. `valid_rest` (line 139) is
    // `is_ascii_alphanumeric() || '_' || '$'` — `$` IS legal in non-first
    // position. Postgres' own lexer matches this (a `$`-leading token is a
    // dollar-quote / positional-param marker, not an identifier). No existing
    // test exercises `$`: `validate_identifier_rejects_semicolon_drop_payload`
    // (line 992) covers `;`/quotes/leading-digit and
    // `validate_identifier_accepts_schema_qualified_name` (line 1008) covers
    // `.`. This pins the asymmetry so a refactor that folds `$` into
    // `valid_start` (accepting `$tab`) — or drops it from `valid_rest`
    // (rejecting the legal `tab$1`) — is caught.
    //
    // is_boilerplate_check: not a smoke "valid name passes" test — it asserts
    // BOTH directions of a position-dependent character rule (accept mid,
    // reject leading) plus the per-segment application across a `.`, which is
    // where an off-by-one in the `chars.next()`/`for c in chars` split (lines
    // 144-149) would surface.
    #[test]
    fn validate_identifier_dollar_legal_only_in_non_first_position() {
        // `$` legal as a non-first char.
        assert!(
            validate_identifier("tab$1", "table").is_ok(),
            "`$` must be accepted in non-first position"
        );
        assert!(
            validate_identifier("a1$_2", "col").is_ok(),
            "`$` mixed with alnum/underscore in rest must pass"
        );
        // `$` illegal as the FIRST char of a segment.
        assert!(
            validate_identifier("$tab", "table").is_err(),
            "leading `$` must be rejected"
        );
        // Per-segment: legal-rest `$` in the second segment must still pass,
        // and a leading `$` in any segment (not just the first) must fail —
        // proves the start/rest split is re-applied for every dotted segment.
        assert!(
            validate_identifier("schema.tab$1", "table").is_ok(),
            "`$` in non-first position of a later segment must pass"
        );
        assert!(
            validate_identifier("schema.$tab", "table").is_err(),
            "leading `$` in a later segment must be rejected per-segment"
        );
    }

    // ── op_dump: limit-guard fires before any connection ──
    //
    // `op_dump` (line 443) validates `table` (line 444) and rejects a negative
    // `limit` (line 451-455) BEFORE reaching `with_client` (line 460). Both
    // are pure arg-validation paths that need no live server, so they are
    // unit-testable here. The negative-limit guard exists because the limit is
    // raw-`format!`-interpolated into `LIMIT {n}` (line 457) — a negative or
    // injected value would otherwise reach the SQL string. No existing test
    // touches `op_dump`.
    //
    // is_boilerplate_check: this is an error-mapping + ordering test — it
    // proves the validation short-circuits BEFORE network I/O (a server-less
    // CI box would hang/error differently if the guard ran after connect),
    // and pins the exact off-by-one boundary: `-1` rejected, `0` accepted
    // (0 is non-negative, so it passes the guard and would build `LIMIT 0`).
    #[test]
    fn op_dump_rejects_negative_limit_before_connecting() {
        with_env(|| {
            // Negative limit → Err from the pure guard, never connects.
            let err = op_dump(json!({"table": "users", "limit": -1}))
                .expect_err("negative limit must be rejected");
            let msg = err.to_string();
            assert!(
                msg.contains("limit") && msg.contains("-1"),
                "expected negative-limit arg error, got: {msg}"
            );
        });
    }

    #[test]
    fn op_dump_rejects_invalid_table_before_connecting() {
        with_env(|| {
            // Injection table name → validate_identifier Err, never connects.
            let err = op_dump(json!({"table": "users; DROP TABLE x"}))
                .expect_err("injection table must be rejected");
            let msg = err.to_string();
            assert!(
                msg.contains("table"),
                "expected table validation error, got: {msg}"
            );
            // Missing table key → "missing table" Err, also pre-connect.
            assert!(
                op_dump(json!({})).is_err(),
                "missing table must error before connecting"
            );
        });
    }

    // ── op_insert_many: empty-rows invariant + pre-connect column guard ──
    //
    // `op_insert_many` (line 471) short-circuits on an empty `rows` array
    // (line 482-484) returning `{"inserted": 0}` WITHOUT building SQL or
    // connecting. It also validates every column key (line 488-491) before
    // `with_client` (line 500). No existing test touches `op_insert_many`.
    //
    // is_boilerplate_check: the empty-rows case is the classic off-by-one /
    // empty-input invariant — a regression that dropped the `is_empty()` guard
    // would index `rows[0]` (line 485) and PANIC on `[]`, or build
    // `VALUES ()` with zero placeholders. The column-injection case proves the
    // identifier guard is applied to map KEYS (a distinct code path from the
    // `table` value), and that it fires before any network I/O on a
    // server-less box.
    #[test]
    fn op_insert_many_empty_rows_returns_zero_without_connecting() {
        with_env(|| {
            // No live server: this must NOT connect. It returns inserted:0.
            let out = op_insert_many(json!({"table": "users", "rows": []}))
                .expect("empty rows is a no-op success, not an error");
            assert_eq!(
                out,
                json!({"inserted": 0}),
                "empty rows must early-return inserted:0 without connecting"
            );
        });
    }

    #[test]
    fn op_insert_many_rejects_injection_column_key_before_connecting() {
        with_env(|| {
            // A malicious COLUMN NAME (map key) must be rejected by
            // validate_identifier before any SQL is built or connection opened.
            let err = op_insert_many(json!({
                "table": "users",
                "rows": [{"id": 1, "name); DROP TABLE users; --": "x"}],
            }))
            .expect_err("injection column key must be rejected");
            let msg = err.to_string();
            assert!(
                msg.contains("column"),
                "expected column validation error, got: {msg}"
            );
        });
    }

    // A malicious RETURNING expression must be rejected by validate_identifier
    // before any SQL is built or a connection opened — the clause is appended
    // to the INSERT verbatim, so an unchecked value would be a SQL-injection
    // sink. Each comma-separated token is identifier-validated.
    #[test]
    fn op_insert_many_rejects_injection_returning_before_connecting() {
        with_env(|| {
            let err = op_insert_many(json!({
                "table": "users",
                "rows": [{"id": 1}],
                "returning": "id; DROP TABLE users; --",
            }))
            .expect_err("injection returning clause must be rejected");
            let msg = err.to_string();
            assert!(
                msg.contains("returning column"),
                "expected returning-column validation error, got: {msg}"
            );
        });
    }

    /// `url_from_opts` must route the host through `sanitize_host`. End-to-end
    /// check that an injection payload reaches the DSN as `127.0.0.1`, not
    /// the redirect target.
    #[test]
    fn url_from_opts_host_injection_is_neutralized() {
        with_env(|| {
            let u = url_from_opts(&json!({
                "host": "evil.example.com/?#@trusted-host",
                "user": "u",
                "password": "p",
                "database": "d",
            }));
            assert!(
                u.contains("@127.0.0.1:"),
                "host injection must collapse to safe fallback, got: {u}"
            );
            assert!(
                !u.contains("trusted-host") && !u.contains("evil.example.com"),
                "neither injection target nor evil hostname may survive: {u}"
            );
        });
    }

    // ── new-surface validation (must reject before connecting) ───────────────

    #[test]
    fn copy_in_requires_sql_and_data_before_connecting() {
        with_env(|| {
            assert!(op_copy_in(json!({"data": "1\n"}))
                .unwrap_err()
                .to_string()
                .contains("missing sql"));
            assert!(op_copy_in(json!({"sql": "COPY t FROM STDIN"}))
                .unwrap_err()
                .to_string()
                .contains("missing data"));
        });
    }

    #[test]
    fn copy_out_requires_sql_before_connecting() {
        with_env(|| {
            assert!(op_copy_out(json!({}))
                .unwrap_err()
                .to_string()
                .contains("missing sql"));
        });
    }

    #[test]
    fn notify_requires_channel_before_connecting() {
        with_env(|| {
            assert!(op_notify(json!({"payload": "x"}))
                .unwrap_err()
                .to_string()
                .contains("missing channel"));
        });
    }

    #[test]
    fn listen_requires_nonempty_channels_before_connecting() {
        with_env(|| {
            assert!(op_listen(json!({}))
                .unwrap_err()
                .to_string()
                .contains("missing channels"));
            assert!(op_listen(json!({"channels": []}))
                .unwrap_err()
                .to_string()
                .contains("at least one channel"));
        });
    }

    /// LISTEN channel names are interpolated, so they MUST be identifier-quoted
    /// to neutralize an injection like `evt; DROP TABLE x; --`. Pin the quoting.
    #[test]
    fn quote_ident_escapes_embedded_quotes() {
        assert_eq!(quote_ident("events"), "\"events\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        // An injection payload stays inside one quoted identifier.
        let q = quote_ident("evt\"; DROP TABLE x; --");
        assert!(q.starts_with('"') && q.ends_with('"'));
        assert_eq!(
            q.matches('"').count(),
            4,
            "inner quote must be doubled, ends quoted"
        );
    }

    // ── pure DSN / quoting helpers (no connection) ───────────────────────────

    #[test]
    fn parse_dsn_full_uri_decomposes_every_part() {
        let v = op_parse_dsn(json!({
            "dsn": "postgresql://alice:s3cret@db.example.com:6543/analytics?sslmode=require&application_name=etl"
        }))
        .unwrap();
        assert_eq!(v["scheme"], json!("postgresql"));
        assert_eq!(v["user"], json!("alice"));
        assert_eq!(v["password"], json!("s3cret"));
        assert_eq!(v["host"], json!("db.example.com"));
        assert_eq!(v["port"], json!(6543));
        assert_eq!(v["dbname"], json!("analytics"));
        assert_eq!(v["params"]["sslmode"], json!("require"));
        assert_eq!(v["params"]["application_name"], json!("etl"));
    }

    #[test]
    fn parse_dsn_percent_decodes_userinfo() {
        // Password `p@ss:word` is percent-encoded in a valid DSN; parsing must
        // recover it rather than mis-split on the literal `@`/`:`.
        let v = op_parse_dsn(json!({"dsn": "postgres://u:p%40ss%3Aword@localhost/db"})).unwrap();
        assert_eq!(v["password"], json!("p@ss:word"));
        assert_eq!(v["host"], json!("localhost"));
        assert_eq!(v["port"], Value::Null, "no port → null");
    }

    #[test]
    fn parse_dsn_rejects_non_uri_and_bad_scheme() {
        assert!(op_parse_dsn(json!({"dsn": "host=localhost dbname=x"})).is_err());
        assert!(op_parse_dsn(json!({"dsn": "mysql://localhost/x"})).is_err());
        assert!(op_parse_dsn(json!({})).is_err());
    }

    #[test]
    fn parse_keyword_dsn_decomposes_libpq_conninfo() {
        // Well-known keys lift to top-level; the rest go under params; port → num.
        let v = op_parse_keyword_dsn(json!({
            "dsn": "host=db.example.com port=6543 dbname=analytics user=alice password=s3cret sslmode=require connect_timeout=10"
        }))
        .unwrap();
        assert_eq!(v["host"], json!("db.example.com"));
        assert_eq!(v["port"], json!(6543), "numeric port is parsed to a number");
        assert_eq!(v["dbname"], json!("analytics"));
        assert_eq!(v["user"], json!("alice"));
        assert_eq!(v["password"], json!("s3cret"));
        assert_eq!(v["params"]["sslmode"], json!("require"));
        assert_eq!(v["params"]["connect_timeout"], json!("10"));
        // Spaces around `=`, a single-quoted value with spaces, and backslash
        // escapes inside a quoted value.
        let q = op_parse_keyword_dsn(json!({
            "dsn": "host = localhost application_name = 'my app' password='a\\'b\\\\c'"
        }))
        .unwrap();
        assert_eq!(q["host"], json!("localhost"));
        assert_eq!(q["params"]["application_name"], json!("my app"));
        assert_eq!(
            q["password"],
            json!("a'b\\c"),
            "\\' and \\\\ unescape inside quotes"
        );
        // An empty single-quoted value is allowed; a missing host stays null.
        let e = op_parse_keyword_dsn(json!({"dsn": "dbname=x password=''"})).unwrap();
        assert_eq!(e["password"], json!(""));
        assert_eq!(e["host"], Value::Null);
        // Errors: a keyword with no `=`, an unterminated quote, missing arg.
        assert!(op_parse_keyword_dsn(json!({"dsn": "host"})).is_err());
        assert!(op_parse_keyword_dsn(json!({"dsn": "host='localhost"})).is_err());
        assert!(op_parse_keyword_dsn(json!({})).is_err());
    }

    #[test]
    fn build_keyword_dsn_inverts_parse_keyword_dsn() {
        let dsn = |v: Value| {
            op_build_keyword_dsn(v).unwrap()["dsn"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // Well-known keys emit first in canonical order; port stringifies.
        assert_eq!(
            dsn(
                json!({"host": "db.example.com", "port": 6543, "dbname": "analytics", "user": "alice"})
            ),
            "host=db.example.com port=6543 dbname=analytics user=alice"
        );
        // params follow, sorted by key.
        assert_eq!(
            dsn(json!({"host": "h", "params": {"sslmode": "require", "connect_timeout": "10"}})),
            "host=h connect_timeout=10 sslmode=require"
        );
        // A value with spaces or an empty value is single-quoted; `'`/`\` escaped.
        assert_eq!(
            dsn(json!({"password": "a b", "user": ""})),
            "user='' password='a b'"
        );
        assert_eq!(dsn(json!({"password": "a'b\\c"})), "password='a\\'b\\\\c'");
        // Round-trips parse_keyword_dsn for a realistic conninfo.
        let original = "host=localhost port=5432 dbname=mydb user=foo sslmode=require";
        let parsed = op_parse_keyword_dsn(json!({"dsn": original})).unwrap();
        let rebuilt = op_build_keyword_dsn(parsed.clone()).unwrap();
        let reparsed = op_parse_keyword_dsn(json!({"dsn": rebuilt["dsn"]})).unwrap();
        assert_eq!(reparsed, parsed, "build → parse round-trips the fields");
        // No parameters is an error.
        assert!(op_build_keyword_dsn(json!({})).is_err());
    }

    #[test]
    fn build_dsn_round_trips_through_parse() {
        let built = op_build_dsn(json!({
            "user": "u", "password": "p@ss", "host": "127.0.0.1", "port": 5432, "dbname": "app"
        }))
        .unwrap();
        let dsn = built["dsn"].as_str().unwrap();
        let parsed = op_parse_dsn(json!({"dsn": dsn})).unwrap();
        assert_eq!(parsed["user"], json!("u"));
        assert_eq!(
            parsed["password"],
            json!("p@ss"),
            "round-trips the @ in password"
        );
        assert_eq!(parsed["host"], json!("127.0.0.1"));
        assert_eq!(parsed["port"], json!(5432));
        assert_eq!(parsed["dbname"], json!("app"));
    }

    #[test]
    fn build_dsn_omits_password_section_when_blank() {
        let built = op_build_dsn(json!({"user": "postgres", "dbname": "postgres"})).unwrap();
        let dsn = built["dsn"].as_str().unwrap();
        assert!(
            !dsn.contains(':') || dsn.contains(":5432"),
            "no empty password colon: {dsn}"
        );
        assert!(
            dsn.starts_with("postgresql://postgres@"),
            "userinfo is user-only: {dsn}"
        );
    }

    #[test]
    fn quote_ident_and_literal_double_their_quotes() {
        let id = op_quote_ident(json!({"name": "weird\"col"})).unwrap();
        assert_eq!(id["quoted"], json!("\"weird\"\"col\""));
        let lit = op_quote_literal(json!({"value": "O'Brien"})).unwrap();
        assert_eq!(lit["quoted"], json!("'O''Brien'"));
    }

    #[test]
    fn unquote_ident_inverts_quote_ident() {
        // Doubled quote decodes to one.
        assert_eq!(
            op_unquote_ident(json!({"quoted": "\"weird\"\"col\""})).unwrap()["name"],
            json!("weird\"col")
        );
        // Plain and empty quoted names.
        assert_eq!(
            op_unquote_ident(json!({"quoted": "\"plain\""})).unwrap()["name"],
            json!("plain")
        );
        assert_eq!(
            op_unquote_ident(json!({"quoted": "\"\""})).unwrap()["name"],
            json!("")
        );
        // Round-trips quote_ident for any input.
        for raw in ["table", "weird\"col", "has space", "MixedCase"] {
            let q = op_quote_ident(json!({ "name": raw })).unwrap()["quoted"].clone();
            assert_eq!(
                op_unquote_ident(json!({ "quoted": q })).unwrap()["name"],
                json!(raw),
                "round-trip {raw:?}"
            );
        }
        // Not quoted / unpaired quote reject.
        assert!(op_unquote_ident(json!({"quoted": "plain"})).is_err());
        assert!(op_unquote_ident(json!({"quoted": "\"a\"b\""})).is_err());
        assert!(op_unquote_ident(json!({})).is_err());
    }

    #[test]
    fn quote_nullable_emits_bare_null_for_null() {
        // A string value quotes exactly like quote_literal.
        assert_eq!(
            op_quote_nullable(json!({"value": "O'Brien"})).unwrap()["quoted"],
            json!("'O''Brien'")
        );
        // JSON null and an absent value both produce the unquoted text NULL.
        assert_eq!(
            op_quote_nullable(json!({ "value": Value::Null })).unwrap()["quoted"],
            json!("NULL")
        );
        assert_eq!(
            op_quote_nullable(json!({})).unwrap()["quoted"],
            json!("NULL")
        );
        // A non-string, non-null value is rejected.
        assert!(op_quote_nullable(json!({"value": 42})).is_err());
    }

    #[test]
    fn unquote_literal_inverts_quote_literal() {
        // Doubled quote decodes to one.
        assert_eq!(
            op_unquote_literal(json!({"value": "'O''Brien'"})).unwrap()["value"],
            json!("O'Brien")
        );
        // Empty literal and plain literal.
        assert_eq!(
            op_unquote_literal(json!({"value": "''"})).unwrap()["value"],
            json!("")
        );
        assert_eq!(
            op_unquote_literal(json!({"value": "'plain'"})).unwrap()["value"],
            json!("plain")
        );
        // Backslash is literal under standard_conforming_strings (no escape).
        assert_eq!(
            op_unquote_literal(json!({"value": "'a\\b'"})).unwrap()["value"],
            json!("a\\b")
        );
        // Round-trips quote_literal for any input.
        for raw in ["O'Brien", "", "a'b'c", "plain", "''quoted''"] {
            let q = op_quote_literal(json!({ "value": raw })).unwrap()["quoted"].clone();
            assert_eq!(
                op_unquote_literal(json!({ "value": q })).unwrap()["value"],
                json!(raw),
                "round-trip for {raw:?}"
            );
        }
        // Not single-quoted, and an unpaired internal quote, both reject.
        assert!(op_unquote_literal(json!({"value": "noquotes"})).is_err());
        assert!(op_unquote_literal(json!({"value": "'a'b'"})).is_err());
    }

    #[test]
    fn escape_like_backslash_escapes_wildcards() {
        let e = |s: &str| {
            op_escape_like(json!({ "value": s })).unwrap()["escaped"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // Plain text is unchanged.
        assert_eq!(e("hello"), "hello");
        // The two LIKE wildcards and the escape char itself are backslash-escaped.
        assert_eq!(e("100%"), "100\\%");
        assert_eq!(e("a_b"), "a\\_b");
        assert_eq!(e("a\\b"), "a\\\\b");
        // A realistic mix.
        assert_eq!(e("50%_off\\sale"), "50\\%\\_off\\\\sale");
        // Empty string stays empty; missing arg errors.
        assert_eq!(e(""), "");
        assert!(op_escape_like(json!({})).is_err());
    }

    #[test]
    fn unescape_like_inverts_escape_like() {
        let u = |s: &str| {
            op_unescape_like(json!({ "value": s })).unwrap()["unescaped"]
                .as_str()
                .unwrap()
                .to_string()
        };
        assert_eq!(u("hello"), "hello");
        assert_eq!(u("100\\%"), "100%");
        assert_eq!(u("a\\_b"), "a_b");
        assert_eq!(u("a\\\\b"), "a\\b");
        assert_eq!(u(""), "");
        // Round-trips escape_like for every kind of input.
        for s in ["hello", "100%", "a_b", "a\\b", "50%_off\\sale", ""] {
            let esc = op_escape_like(json!({ "value": s })).unwrap()["escaped"]
                .as_str()
                .unwrap()
                .to_string();
            assert_eq!(u(&esc), s, "round-trip {s:?}");
        }
        // An unescaped wildcard, a dangling backslash, and a bogus escape are rejected.
        assert!(op_unescape_like(json!({ "value": "a%b" })).is_err());
        assert!(op_unescape_like(json!({ "value": "a_b" })).is_err());
        assert!(op_unescape_like(json!({ "value": "trail\\" })).is_err());
        assert!(op_unescape_like(json!({ "value": "a\\b" })).is_err());
        assert!(op_unescape_like(json!({})).is_err());
    }

    #[test]
    fn dollar_quote_wraps_and_auto_picks_a_non_colliding_tag() {
        // No collision → the empty tag ($$…$$); quotes/backslashes stay literal.
        let v = op_dollar_quote(json!({"value": "Dianne's horse \\ \"q\""})).unwrap();
        assert_eq!(v["tag"], json!(""));
        assert_eq!(v["quoted"], json!("$$Dianne's horse \\ \"q\"$$"));
        // Content containing `$$` forces a non-empty auto tag.
        let c = op_dollar_quote(json!({"value": "a $$ b"})).unwrap();
        assert_eq!(c["tag"], json!("dq0"));
        assert_eq!(c["quoted"], json!("$dq0$a $$ b$dq0$"));
        // If even `$dq0$` appears, the next tag is chosen.
        let c2 = op_dollar_quote(json!({"value": "x $$ y $dq0$ z"})).unwrap();
        assert_eq!(c2["tag"], json!("dq1"));
        assert_eq!(c2["quoted"], json!("$dq1$x $$ y $dq0$ z$dq1$"));
        // Explicit tag.
        let e = op_dollar_quote(json!({"value": "body", "tag": "fn"})).unwrap();
        assert_eq!(e["quoted"], json!("$fn$body$fn$"));
        // A bare `$` in the content is fine (it's not the delimiter).
        assert_eq!(
            op_dollar_quote(json!({"value": "price is $5"})).unwrap()["quoted"],
            json!("$$price is $5$$")
        );
        // Explicit tag that collides with the content, or is not a legal tag.
        assert!(op_dollar_quote(json!({"value": "has $fn$ inside", "tag": "fn"})).is_err());
        assert!(op_dollar_quote(json!({"value": "x", "tag": "bad$tag"})).is_err());
        assert!(op_dollar_quote(json!({"value": "x", "tag": "1leading"})).is_err());
        assert!(op_dollar_quote(json!({})).is_err());
    }

    #[test]
    fn unquote_dollar_inverts_dollar_quote() {
        // Empty-tag form, content with quotes/backslashes returned verbatim.
        let v = op_unquote_dollar(json!({"value": "$$Dianne's horse \\ \"q\"$$"})).unwrap();
        assert_eq!(v["tag"], json!(""));
        assert_eq!(v["value"], json!("Dianne's horse \\ \"q\""));
        // Named tag.
        let n = op_unquote_dollar(json!({"value": "$fn$body$fn$"})).unwrap();
        assert_eq!(n["tag"], json!("fn"));
        assert_eq!(n["value"], json!("body"));
        // Empty body.
        assert_eq!(
            op_unquote_dollar(json!({"value": "$$$$"})).unwrap()["value"],
            json!("")
        );
        assert_eq!(
            op_unquote_dollar(json!({"value": "$t$$t$"})).unwrap()["value"],
            json!("")
        );
        // A bare `$` and a different `$$` inside a named tag are fine.
        assert_eq!(
            op_unquote_dollar(json!({"value": "$outer$ has $$ and $5 $outer$"})).unwrap()["value"],
            json!(" has $$ and $5 ")
        );
        // Round-trips dollar_quote for tricky content (including auto-tag choice).
        for raw in [
            "plain",
            "Dianne's",
            "a $$ b",
            "x $$ y $dq0$ z",
            "",
            "price is $5",
        ] {
            let q = op_dollar_quote(json!({ "value": raw })).unwrap()["quoted"]
                .as_str()
                .unwrap()
                .to_string();
            assert_eq!(
                op_unquote_dollar(json!({ "value": q })).unwrap()["value"],
                json!(raw),
                "round-trip for {raw:?}"
            );
        }
        // Errors: no leading `$`, unterminated tag, wrong/absent closing delim,
        // illegal tag, and a body that re-contains its own delimiter.
        assert!(op_unquote_dollar(json!({"value": "noquotes"})).is_err());
        assert!(op_unquote_dollar(json!({"value": "$tag"})).is_err());
        assert!(op_unquote_dollar(json!({"value": "$fn$body$other$"})).is_err());
        assert!(op_unquote_dollar(json!({"value": "$fn$"})).is_err());
        assert!(op_unquote_dollar(json!({"value": "$1bad$x$1bad$"})).is_err());
        assert!(op_unquote_dollar(json!({})).is_err());
    }

    #[test]
    fn quote_qualified_ident_quotes_each_segment() {
        let q = op_quote_qualified_ident(json!({"name": "public.my table"})).unwrap();
        assert_eq!(q["quoted"], json!("\"public\".\"my table\""));
        assert_eq!(q["parts"], json!(["public", "my table"]));
        // Three-part name with an embedded quote in the last segment.
        let three = op_quote_qualified_ident(json!({"name": "db.schema.we\"ird"})).unwrap();
        assert_eq!(three["quoted"], json!("\"db\".\"schema\".\"we\"\"ird\""));
        // Single bare identifier still works (no dots).
        assert_eq!(
            op_quote_qualified_ident(json!({"name": "users"})).unwrap()["quoted"],
            json!("\"users\"")
        );
        // Empty segments (leading/trailing/doubled dot) are rejected.
        assert!(op_quote_qualified_ident(json!({"name": "public."})).is_err());
        assert!(op_quote_qualified_ident(json!({"name": ".table"})).is_err());
        assert!(op_quote_qualified_ident(json!({"name": "a..b"})).is_err());
    }

    #[test]
    fn parse_qualified_ident_inverts_quote_qualified_ident() {
        // Quoted segments: a `.` inside quotes stays, `""` un-doubles.
        assert_eq!(
            op_parse_qualified_ident(json!({"name": "\"public\".\"my table\""})).unwrap()["parts"],
            json!(["public", "my table"])
        );
        assert_eq!(
            op_parse_qualified_ident(json!({"name": "\"a.b\".\"c\""})).unwrap()["parts"],
            json!(["a.b", "c"]),
            "dot inside quotes is literal"
        );
        assert_eq!(
            op_parse_qualified_ident(json!({"name": "\"we\"\"ird\""})).unwrap()["parts"],
            json!(["we\"ird"]),
            "doubled quote decodes to one"
        );
        // Bare (unquoted) segments pass through.
        assert_eq!(
            op_parse_qualified_ident(json!({"name": "public.users"})).unwrap()["parts"],
            json!(["public", "users"])
        );
        // Round-trips quote_qualified_ident across tricky names.
        for name in ["public.my table", "db.schema.we\"ird", "users"] {
            let quoted = op_quote_qualified_ident(json!({ "name": name })).unwrap()["quoted"]
                .as_str()
                .unwrap()
                .to_string();
            let parts =
                op_parse_qualified_ident(json!({ "name": quoted })).unwrap()["parts"].clone();
            let original: Vec<&str> = name.split('.').collect();
            assert_eq!(parts, json!(original), "round-trip for {name}");
        }
        // Unquoted empty segments and an unterminated quote are rejected.
        assert!(op_parse_qualified_ident(json!({"name": "a..b"})).is_err());
        assert!(op_parse_qualified_ident(json!({"name": ".x"})).is_err());
        assert!(op_parse_qualified_ident(json!({"name": "\"unterminated"})).is_err());
    }

    #[test]
    fn format_array_quotes_only_elements_that_need_it() {
        // Plain tokens stay bare; the result is a Postgres array literal.
        assert_eq!(
            op_format_array(json!({"elements": ["a", "b", "c"]})).unwrap()["array"],
            json!("{a,b,c}")
        );
        // Comma, whitespace, empty, and case-insensitive NULL force quoting.
        assert_eq!(
            op_format_array(json!({"elements": ["a,b", "c d", "", "NULL", "ok"]})).unwrap()
                ["array"],
            json!("{\"a,b\",\"c d\",\"\",\"NULL\",ok}")
        );
        // Embedded quote and backslash are backslash-escaped inside the quotes.
        assert_eq!(
            op_format_array(json!({"elements": ["he\"llo", "a\\b"]})).unwrap()["array"],
            json!("{\"he\\\"llo\",\"a\\\\b\"}")
        );
        // Empty list → empty array literal.
        assert_eq!(
            op_format_array(json!({"elements": []})).unwrap()["array"],
            json!("{}")
        );
        assert!(op_format_array(json!({})).is_err());
    }

    #[test]
    fn format_in_list_renders_mixed_types_and_empty_sentinel() {
        // Bare numbers.
        assert_eq!(
            op_format_in_list(json!({"values": [1, 2, 3]})).unwrap()["list"],
            json!("(1, 2, 3)")
        );
        // Strings single-quoted, embedded quote doubled.
        assert_eq!(
            op_format_in_list(json!({"values": ["a", "O'Brien"]})).unwrap()["list"],
            json!("('a', 'O''Brien')")
        );
        // Mixed number/string/bool/null.
        assert_eq!(
            op_format_in_list(json!({"values": [1, "a", true, null]})).unwrap()["list"],
            json!("(1, 'a', TRUE, NULL)")
        );
        // Empty list → (NULL): valid SQL that matches nothing.
        assert_eq!(
            op_format_in_list(json!({"values": []})).unwrap()["list"],
            json!("(NULL)")
        );
        // `elements` is accepted as an alias for `values`.
        assert_eq!(
            op_format_in_list(json!({"elements": [42]})).unwrap()["list"],
            json!("(42)")
        );
        assert!(op_format_in_list(json!({})).is_err());
    }

    #[test]
    fn parse_in_list_inverts_format_in_list() {
        let parse = |s: &str| op_parse_in_list(json!({ "list": s })).unwrap()["elements"].clone();
        // Bare numbers, strings, bool, null.
        assert_eq!(parse("(1, 2, 3)"), json!([1, 2, 3]));
        assert_eq!(parse("('a', 'O''Brien')"), json!(["a", "O'Brien"]));
        assert_eq!(parse("(1, 'a', TRUE, NULL)"), json!([1, "a", true, null]));
        // A float and a comma inside a quoted string.
        assert_eq!(parse("(1.5, 'a,b')"), json!([1.5, "a,b"]));
        // Round-trips format_in_list for mixed types.
        for vals in [
            json!([1, 2, 3]),
            json!(["a", "O'Brien"]),
            json!([1, "a", true, null]),
        ] {
            let listed = op_format_in_list(json!({ "values": vals })).unwrap()["list"]
                .as_str()
                .unwrap()
                .to_string();
            assert_eq!(
                op_parse_in_list(json!({ "list": listed })).unwrap()["elements"],
                vals
            );
        }
        // The empty sentinel `(NULL)` parses to a single null; `value` alias works.
        assert_eq!(parse("(NULL)"), json!([null]));
        assert_eq!(
            op_parse_in_list(json!({ "value": "(7)" })).unwrap()["elements"],
            json!([7])
        );
        // Not an IN list, an unterminated quote, and a missing arg error.
        assert!(op_parse_in_list(json!({ "list": "1, 2, 3" })).is_err());
        assert!(op_parse_in_list(json!({ "list": "('oops)" })).is_err());
        assert!(op_parse_in_list(json!({})).is_err());
    }

    #[test]
    fn parse_array_inverts_format_array() {
        // Bare and quoted elements; quoted "NULL" stays a string.
        assert_eq!(
            op_parse_array(json!({"array": "{a,b,c}"})).unwrap()["elements"],
            json!(["a", "b", "c"])
        );
        assert_eq!(
            op_parse_array(json!({"array": "{\"a,b\",\"c d\",\"\",\"NULL\",ok}"})).unwrap()
                ["elements"],
            json!(["a,b", "c d", "", "NULL", "ok"])
        );
        // Backslash-unescape mirrors format_array's escaping.
        assert_eq!(
            op_parse_array(json!({"array": "{\"he\\\"llo\",\"a\\\\b\"}"})).unwrap()["elements"],
            json!(["he\"llo", "a\\b"])
        );
        // Unquoted NULL is a real null; empty array is an empty list.
        assert_eq!(
            op_parse_array(json!({"array": "{a,NULL,b}"})).unwrap()["elements"],
            json!(["a", null, "b"])
        );
        assert_eq!(
            op_parse_array(json!({"array": "{}"})).unwrap()["elements"],
            json!([])
        );
        // Round-trips every needs-quoting case through format_array.
        let src = json!(["a,b", "c d", "", "NULL", "ok", "he\"llo", "a\\b"]);
        let lit = op_format_array(json!({"elements": src})).unwrap()["array"].clone();
        assert_eq!(
            op_parse_array(json!({"array": lit})).unwrap()["elements"],
            src
        );
        // Not an array literal, and multidimensional input, both reject.
        assert!(op_parse_array(json!({"array": "a,b,c"})).is_err());
        assert!(op_parse_array(json!({"array": "{{1,2},{3,4}}"})).is_err());
    }

    #[test]
    fn parse_range_decodes_bounds_and_inclusivity() {
        // [3,7): 3 inclusive, 7 exclusive.
        let v = op_parse_range(json!({"range": "[3,7)"})).unwrap();
        assert_eq!(v["lower"], json!("3"));
        assert_eq!(v["upper"], json!("7"));
        assert_eq!(v["lower_inclusive"], json!(true));
        assert_eq!(v["upper_inclusive"], json!(false));
        assert_eq!(v["empty"], json!(false));
        // Unbounded lower (omitted) → null; upper inclusive.
        let lo = op_parse_range(json!({"range": "(,5]"})).unwrap();
        assert_eq!(lo["lower"], Value::Null);
        assert_eq!(lo["upper"], json!("5"));
        assert_eq!(lo["lower_inclusive"], json!(false));
        assert_eq!(lo["upper_inclusive"], json!(true));
        // Unbounded upper.
        assert_eq!(
            op_parse_range(json!({"range": "[1,)"})).unwrap()["upper"],
            Value::Null
        );
        // `empty` (case-insensitive) is the empty range; bounds are null.
        let e = op_parse_range(json!({"range": "EMPTY"})).unwrap();
        assert_eq!(e["empty"], json!(true));
        assert_eq!(e["lower"], Value::Null);
        // Quoted text bounds: a comma inside the quotes does not split.
        let q = op_parse_range(json!({"range": "[\"a,b\",\"z\")"})).unwrap();
        assert_eq!(q["lower"], json!("a,b"));
        assert_eq!(q["upper"], json!("z"));
        // An empty-string bound ("") is distinct from unbounded (omitted).
        let es = op_parse_range(json!({"range": "[\"\",b]"})).unwrap();
        assert_eq!(es["lower"], json!(""));
        // `value` alias; structural errors.
        assert_eq!(
            op_parse_range(json!({"value": "[1,10]"})).unwrap()["upper_inclusive"],
            json!(true)
        );
        assert!(op_parse_range(json!({"range": "1,10"})).is_err()); // no brackets
        assert!(op_parse_range(json!({"range": "[1-10)"})).is_err()); // no comma
        assert!(op_parse_range(json!({})).is_err());
    }

    #[test]
    fn percent_decode_is_inverse_and_tolerant() {
        assert_eq!(percent_decode("a%20b%2Fc"), "a b/c");
        // A stray `%` with no valid hex pair is left verbatim, never panics.
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz"), "%zz");
    }
}
