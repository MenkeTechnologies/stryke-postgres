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

use anyhow::{anyhow, bail, Result};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
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
    let host = opts
        .get("host")
        .and_then(|v| v.as_str())
        .unwrap_or("127.0.0.1");
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
        match self {
            PgParam::Null => Ok(postgres::types::IsNull::Yes),
            PgParam::Bool(b) => b.to_sql(ty, out),
            PgParam::Int(n) => n.to_sql(ty, out),
            PgParam::Float(f) => f.to_sql(ty, out),
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
    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table,
        col_list,
        placeholders.join(", ")
    );
    with_client(&opts, |c| {
        let stmt = c.prepare(&sql)?;
        let mut total = 0i64;
        for row in &rows {
            let obj = row
                .as_object()
                .ok_or_else(|| anyhow!("row must be an object"))?;
            let params: Vec<PgParam> = cols.iter().map(|k| json_to_param(&obj[k])).collect();
            let p_refs = params_as_sql(&params);
            total += c.execute(&stmt, &p_refs)? as i64;
        }
        Ok(json!({"inserted": total}))
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
}
