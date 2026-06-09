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

use anyhow::{anyhow, Result};
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
        user.to_string()
    } else {
        format!("{}:{}", user, password)
    };
    format!("postgresql://{}@{}:{}/{}", auth, host, port, db)
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
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?
        .to_string();
    let limit = opts["limit"].as_i64();
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
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?
        .to_string();
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
    let cols: Vec<String> = first.keys().cloned().collect();
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
}
