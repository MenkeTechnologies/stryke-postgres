//! `stryke-postgres-helper` — bridge binary for the stryke `postgres` package.
//!
//! Single-shot model: every invocation opens a fresh Postgres connection,
//! runs one command, prints JSON, exits. Mirror of stryke-mysql-helper but
//! for Postgres (`$1`/`$2` placeholders, no `last_insert_id`, jsonb/uuid/
//! timestamp native encoding, etc.).
//!
//! Also supports a `serve` JSON-RPC daemon on a Unix socket for hot loops
//! once stryke gains a Unix-socket client builtin (currently single-shot
//! only on the stryke side).

use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use clap::{Parser, Subcommand};
use postgres::types::{ToSql, Type};
use postgres::{Client, Config, NoTls, Row};
use serde_json::{json, Map as JMap, Value};
use uuid::Uuid;

/* ------------------------------------------------------------------------- */
/* CLI                                                                       */
/* ------------------------------------------------------------------------- */

#[derive(Parser)]
#[command(
    name = "stryke-postgres-helper",
    version,
    about = "PostgreSQL bridge for the stryke `postgres` package"
)]
struct Cli {
    /// DSN URL: `postgres://user:pass@host:port/db?sslmode=require`.
    #[arg(long, env = "POSTGRES_DSN", global = true)]
    dsn: Option<String>,

    /// libpq-style alternate (psql respects this too).
    #[arg(long, env = "DATABASE_URL", global = true)]
    database_url: Option<String>,

    #[arg(long, short = 'H', env = "POSTGRES_HOST", global = true)]
    host: Option<String>,

    #[arg(long, short = 'P', env = "POSTGRES_PORT", global = true)]
    port: Option<u16>,

    #[arg(long, short = 'u', env = "POSTGRES_USER", global = true)]
    user: Option<String>,

    /// Prefer `$POSTGRES_PASSWORD` rather than passing on the CLI.
    #[arg(
        long,
        short = 'p',
        env = "POSTGRES_PASSWORD",
        global = true,
        hide_env_values = true
    )]
    password: Option<String>,

    #[arg(long, short = 'D', env = "POSTGRES_DATABASE", global = true)]
    database: Option<String>,

    /// Connect timeout, seconds.
    #[arg(long, global = true, default_value_t = 10)]
    connect_timeout: u64,

    /// Application name visible in `pg_stat_activity`.
    #[arg(long, global = true, default_value = "stryke-postgres-helper")]
    application_name: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a SELECT and stream rows as NDJSON.
    Query {
        sql: String,
        /// JSON array of bind values (positional `$1`, `$2`, ...).
        #[arg(long)]
        bind: Option<String>,
        /// Emit one columnar JSON object instead of NDJSON.
        #[arg(long)]
        columnar: bool,
        #[arg(long)]
        with_meta: bool,
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Run a single non-SELECT statement.
    Execute {
        sql: String,
        #[arg(long)]
        bind: Option<String>,
    },
    /// Run a multi-statement SQL file. Returns one JSON object per statement.
    Exec {
        #[arg(long, short = 'f')]
        file: PathBuf,
    },
    /// `SELECT * FROM TABLE [WHERE w] [ORDER BY o] [LIMIT n]` shorthand.
    Dump {
        #[arg(long, short = 't')]
        table: String,
        #[arg(long)]
        columns: Option<String>,
        #[arg(long = "where", short = 'w')]
        where_clause: Option<String>,
        #[arg(long)]
        order_by: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
    },
    /// List tables in the current schema.
    Tables {
        /// Schema name (default: current_schema()).
        #[arg(long)]
        schema: Option<String>,
    },
    /// List databases the user can see.
    Databases,
    /// Column + index metadata for a table.
    Schema {
        #[arg(long, short = 't')]
        table: String,
        /// Schema name (default: current_schema()).
        #[arg(long, short = 's')]
        schema: Option<String>,
    },
    /// Run `SELECT 1`. Exit 0/non-zero.
    Ping,
    /// JSON-RPC daemon on a Unix socket.
    Serve {
        #[arg(long = "socket-path")]
        socket_path: PathBuf,
    },
}

/* ------------------------------------------------------------------------- */
/* main                                                                      */
/* ------------------------------------------------------------------------- */

fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(&cli) {
        eprintln!("stryke-postgres-helper: {e:#}");
        std::process::exit(1);
    }
}

fn run(cli: &Cli) -> Result<()> {
    match &cli.cmd {
        Cmd::Query {
            sql,
            bind,
            columnar,
            with_meta,
            limit,
        } => {
            let mut client = connect(cli)?;
            cmd_query(&mut client, sql, bind.as_deref(), *columnar, *with_meta, *limit)
        }
        Cmd::Execute { sql, bind } => {
            let mut client = connect(cli)?;
            let r = exec_execute(&mut client, sql, bind.as_deref())?;
            emit_json(&r)
        }
        Cmd::Exec { file } => {
            let mut client = connect(cli)?;
            cmd_exec_file(&mut client, file)
        }
        Cmd::Dump {
            table,
            columns,
            where_clause,
            order_by,
            limit,
        } => {
            let mut client = connect(cli)?;
            cmd_dump(
                &mut client,
                table,
                columns.as_deref(),
                where_clause.as_deref(),
                order_by.as_deref(),
                *limit,
            )
        }
        Cmd::Tables { schema } => {
            let mut client = connect(cli)?;
            cmd_tables(&mut client, schema.as_deref())
        }
        Cmd::Databases => {
            let mut client = connect(cli)?;
            cmd_databases(&mut client)
        }
        Cmd::Schema { table, schema } => {
            let mut client = connect(cli)?;
            cmd_schema(&mut client, table, schema.as_deref())
        }
        Cmd::Ping => {
            let mut client = connect(cli)?;
            let _row = client.query_one("SELECT 1", &[])?;
            println!("ok");
            Ok(())
        }
        Cmd::Serve { socket_path } => cmd_serve(cli, socket_path),
    }
}

/* ------------------------------------------------------------------------- */
/* connection plumbing                                                       */
/* ------------------------------------------------------------------------- */

fn build_config(cli: &Cli) -> Result<Config> {
    let url = cli.dsn.as_deref().or(cli.database_url.as_deref());
    let mut cfg = if let Some(u) = url {
        Config::from_str(u).context("parsing connection URL")?
    } else {
        Config::new()
    };

    if let Some(h) = &cli.host {
        cfg.host(h);
    }
    if let Some(p) = cli.port {
        cfg.port(p);
    }
    if let Some(u) = &cli.user {
        cfg.user(u);
    }
    if let Some(pw) = &cli.password {
        cfg.password(pw);
    }
    if let Some(db) = &cli.database {
        cfg.dbname(db);
    }
    cfg.connect_timeout(Duration::from_secs(cli.connect_timeout));
    cfg.application_name(&cli.application_name);

    // Default host=localhost if nothing was set (otherwise postgres crate
    // panics on connect with no host).
    if cfg.get_hosts().is_empty() {
        cfg.host("localhost");
    }
    if cfg.get_user().is_none() {
        // Fall back to $USER like libpq.
        if let Ok(u) = std::env::var("USER") {
            cfg.user(&u);
        }
    }
    Ok(cfg)
}

fn connect(cli: &Cli) -> Result<Client> {
    let cfg = build_config(cli)?;
    cfg.connect(NoTls).context("connecting to postgres")
}

/* ------------------------------------------------------------------------- */
/* bind decoding                                                             */
/* ------------------------------------------------------------------------- */

/// Owned wrapper so `Vec<BindVal>` lives long enough to hand `&dyn ToSql`
/// references to `client.query/execute`.
enum BindVal {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(String),
    Json(Value),
}

impl BindVal {
    fn from_json(v: Value) -> BindVal {
        match v {
            Value::Null => BindVal::Null,
            Value::Bool(b) => BindVal::Bool(b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    BindVal::I64(i)
                } else if let Some(f) = n.as_f64() {
                    BindVal::F64(f)
                } else {
                    BindVal::Str(n.to_string())
                }
            }
            Value::String(s) => BindVal::Str(s),
            Value::Array(_) | Value::Object(_) => BindVal::Json(v),
        }
    }
}

impl postgres::types::ToSql for BindVal {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut postgres::types::private::BytesMut,
    ) -> std::result::Result<postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    {
        match self {
            BindVal::Null => Ok(postgres::types::IsNull::Yes),
            BindVal::Bool(b) => b.to_sql(ty, out),
            BindVal::I64(i) => {
                // pg infers the target type from the SQL context; we narrow
                // our JSON i64 to whatever pg asked for so a `WHERE id = $1`
                // against an INT4 column doesn't blow up with "incorrect
                // binary data format".
                if *ty == Type::INT2 {
                    (*i as i16).to_sql(ty, out)
                } else if *ty == Type::INT4 {
                    (*i as i32).to_sql(ty, out)
                } else if *ty == Type::FLOAT4 {
                    (*i as f32).to_sql(ty, out)
                } else if *ty == Type::FLOAT8 {
                    (*i as f64).to_sql(ty, out)
                } else if *ty == Type::OID {
                    (*i as u32).to_sql(ty, out)
                } else if *ty == Type::TEXT
                    || *ty == Type::VARCHAR
                    || *ty == Type::NAME
                    || *ty == Type::BPCHAR
                    || *ty == Type::NUMERIC
                    || *ty == Type::UNKNOWN
                {
                    i.to_string().to_sql(ty, out)
                } else {
                    i.to_sql(ty, out)
                }
            }
            BindVal::F64(f) => {
                if *ty == Type::FLOAT4 {
                    (*f as f32).to_sql(ty, out)
                } else if *ty == Type::NUMERIC
                    || *ty == Type::TEXT
                    || *ty == Type::VARCHAR
                    || *ty == Type::UNKNOWN
                {
                    f.to_string().to_sql(ty, out)
                } else {
                    f.to_sql(ty, out)
                }
            }
            BindVal::Str(s) => s.to_sql(ty, out),
            BindVal::Json(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    postgres::types::to_sql_checked!();
}

impl std::fmt::Debug for BindVal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BindVal::Null => write!(f, "Null"),
            BindVal::Bool(b) => write!(f, "Bool({b})"),
            BindVal::I64(i) => write!(f, "I64({i})"),
            BindVal::F64(x) => write!(f, "F64({x})"),
            BindVal::Str(s) => write!(f, "Str({:?})", s),
            BindVal::Json(v) => write!(f, "Json({v})"),
        }
    }
}

fn parse_bind(s: Option<&str>) -> Result<Vec<BindVal>> {
    let Some(raw) = s else {
        return Ok(Vec::new());
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let v: Value = serde_json::from_str(raw).context("parsing --bind JSON")?;
    match v {
        Value::Array(arr) => Ok(arr.into_iter().map(BindVal::from_json).collect()),
        Value::Null => Ok(Vec::new()),
        _ => bail!("--bind must be a JSON array (Postgres uses positional $1, $2, ...)"),
    }
}

fn bind_refs<'a>(b: &'a [BindVal]) -> Vec<&'a (dyn ToSql + Sync)> {
    b.iter().map(|v| v as &(dyn ToSql + Sync)).collect()
}

/* ------------------------------------------------------------------------- */
/* row → JSON                                                                */
/* ------------------------------------------------------------------------- */

fn row_to_json(row: &Row) -> Value {
    let mut out = JMap::with_capacity(row.columns().len());
    for (i, col) in row.columns().iter().enumerate() {
        out.insert(col.name().to_string(), pgval_to_json(row, i));
    }
    Value::Object(out)
}

fn pgval_to_json(row: &Row, idx: usize) -> Value {
    let col = &row.columns()[idx];
    let ty = col.type_();
    match *ty {
        Type::BOOL => row.try_get::<_, Option<bool>>(idx).ok().flatten()
            .map(|v| json!(v)).unwrap_or(Value::Null),
        Type::INT2 => row.try_get::<_, Option<i16>>(idx).ok().flatten()
            .map(|v| json!(v)).unwrap_or(Value::Null),
        Type::INT4 => row.try_get::<_, Option<i32>>(idx).ok().flatten()
            .map(|v| json!(v)).unwrap_or(Value::Null),
        Type::INT8 => row.try_get::<_, Option<i64>>(idx).ok().flatten()
            .map(|v| json!(v)).unwrap_or(Value::Null),
        Type::OID => row.try_get::<_, Option<u32>>(idx).ok().flatten()
            .map(|v| json!(v)).unwrap_or(Value::Null),
        Type::FLOAT4 => row.try_get::<_, Option<f32>>(idx).ok().flatten()
            .map(|v| json!(v)).unwrap_or(Value::Null),
        Type::FLOAT8 => row.try_get::<_, Option<f64>>(idx).ok().flatten()
            .map(|v| json!(v)).unwrap_or(Value::Null),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::CHAR
            => row.try_get::<_, Option<String>>(idx).ok().flatten()
                .map(|v| json!(v)).unwrap_or(Value::Null),
        Type::JSON | Type::JSONB => row.try_get::<_, Option<Value>>(idx).ok().flatten()
            .unwrap_or(Value::Null),
        Type::UUID => row.try_get::<_, Option<Uuid>>(idx).ok().flatten()
            .map(|v| json!(v.to_string())).unwrap_or(Value::Null),
        Type::DATE => row.try_get::<_, Option<NaiveDate>>(idx).ok().flatten()
            .map(|v| json!(v.format("%Y-%m-%d").to_string())).unwrap_or(Value::Null),
        Type::TIME => row.try_get::<_, Option<NaiveTime>>(idx).ok().flatten()
            .map(|v| json!(v.format("%H:%M:%S%.6f").to_string())).unwrap_or(Value::Null),
        Type::TIMESTAMP => row.try_get::<_, Option<NaiveDateTime>>(idx).ok().flatten()
            .map(|v| json!(v.format("%Y-%m-%d %H:%M:%S%.6f").to_string()))
            .unwrap_or(Value::Null),
        Type::TIMESTAMPTZ => row.try_get::<_, Option<DateTime<Utc>>>(idx).ok().flatten()
            .map(|v| json!(v.to_rfc3339())).unwrap_or(Value::Null),
        Type::BYTEA => row.try_get::<_, Option<Vec<u8>>>(idx).ok().flatten()
            .map(|v| {
                let mut s = String::from("base64:");
                s.push_str(&B64.encode(&v));
                json!(s)
            })
            .unwrap_or(Value::Null),
        Type::BOOL_ARRAY => array_to_json::<bool>(row, idx),
        Type::INT2_ARRAY => array_to_json::<i16>(row, idx),
        Type::INT4_ARRAY => array_to_json::<i32>(row, idx),
        Type::INT8_ARRAY => array_to_json::<i64>(row, idx),
        Type::FLOAT4_ARRAY => array_to_json::<f32>(row, idx),
        Type::FLOAT8_ARRAY => array_to_json::<f64>(row, idx),
        Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => array_to_json::<String>(row, idx),
        _ => {
            // Fallback for NUMERIC, INET, ranges, custom enums, etc.:
            // ask Postgres to cast it to text on its side. We can't do that
            // retroactively here — try fetching as String which works for
            // types that have a text-cast FromSql impl. If that fails, emit
            // null with a stringified-type marker so it's clear to the user.
            if let Ok(Some(s)) = row.try_get::<_, Option<String>>(idx) {
                json!(s)
            } else {
                json!(format!("unsupported:{}", ty.name()))
            }
        }
    }
}

fn array_to_json<T>(row: &Row, idx: usize) -> Value
where
    T: postgres::types::FromSqlOwned + serde::Serialize,
{
    match row.try_get::<_, Option<Vec<Option<T>>>>(idx) {
        Ok(Some(v)) => json!(v),
        _ => Value::Null,
    }
}

/* ------------------------------------------------------------------------- */
/* commands                                                                  */
/* ------------------------------------------------------------------------- */

fn cmd_query(
    client: &mut Client,
    sql: &str,
    bind: Option<&str>,
    columnar: bool,
    with_meta: bool,
    limit: Option<usize>,
) -> Result<()> {
    let binds = parse_bind(bind)?;
    let refs = bind_refs(&binds);
    let rows = client.query(sql, &refs).context("query")?;

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    let columns: Vec<String> = rows
        .first()
        .map(|r| r.columns().iter().map(|c| c.name().to_string()).collect())
        .unwrap_or_default();

    if columnar {
        let mut rows_json: Vec<Value> = Vec::with_capacity(rows.len());
        let mut count = 0usize;
        for row in &rows {
            let mut arr: Vec<Value> = Vec::with_capacity(row.columns().len());
            for i in 0..row.columns().len() {
                arr.push(pgval_to_json(row, i));
            }
            rows_json.push(Value::Array(arr));
            count += 1;
            if let Some(l) = limit {
                if count >= l {
                    break;
                }
            }
        }
        let obj = json!({
            "columns": columns,
            "num_rows": rows_json.len(),
            "rows": rows_json,
        });
        serde_json::to_writer(&mut out, &obj)?;
        out.write_all(b"\n")?;
    } else {
        if with_meta {
            let meta = json!({ "meta": { "columns": columns } });
            serde_json::to_writer(&mut out, &meta)?;
            out.write_all(b"\n")?;
        }
        let mut count = 0usize;
        for row in &rows {
            let v = row_to_json(row);
            serde_json::to_writer(&mut out, &v)?;
            out.write_all(b"\n")?;
            count += 1;
            if let Some(l) = limit {
                if count >= l {
                    break;
                }
            }
        }
    }
    out.flush()?;
    Ok(())
}

#[derive(serde::Serialize)]
struct ExecResult {
    affected_rows: u64,
}

fn exec_execute(client: &mut Client, sql: &str, bind: Option<&str>) -> Result<ExecResult> {
    let binds = parse_bind(bind)?;
    let refs = bind_refs(&binds);
    let n = client.execute(sql, &refs).context("execute")?;
    Ok(ExecResult { affected_rows: n })
}

fn cmd_exec_file(client: &mut Client, file: &PathBuf) -> Result<()> {
    let raw = fs::read_to_string(file)
        .with_context(|| format!("reading {}", file.display()))?;
    // simple_query handles `;`-separated multi-statement scripts in one shot
    // without prepared-statement support — exactly what we want for migrations.
    let results = client
        .simple_query(&raw)
        .context("simple_query")?;
    let mut counts: Vec<Value> = Vec::new();
    for msg in &results {
        match msg {
            postgres::SimpleQueryMessage::CommandComplete(n) => {
                counts.push(json!({ "affected_rows": *n }));
            }
            postgres::SimpleQueryMessage::Row(_) => {} // ignored for exec
            _ => {}
        }
    }
    emit_json(&Value::Array(counts))
}

fn cmd_dump(
    client: &mut Client,
    table: &str,
    columns: Option<&str>,
    where_clause: Option<&str>,
    order_by: Option<&str>,
    limit: Option<usize>,
) -> Result<()> {
    let cols = columns.unwrap_or("*");
    let mut sql = format!("SELECT {} FROM {}", cols, quote_ident(table));
    if let Some(w) = where_clause {
        sql.push_str(" WHERE ");
        sql.push_str(w);
    }
    if let Some(o) = order_by {
        sql.push_str(" ORDER BY ");
        sql.push_str(o);
    }
    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {}", n));
    }
    cmd_query(client, &sql, None, false, false, None)
}

fn cmd_tables(client: &mut Client, schema: Option<&str>) -> Result<()> {
    let sql = "SELECT tablename FROM pg_tables \
               WHERE schemaname = COALESCE($1, current_schema()) \
               ORDER BY tablename";
    let schema_param: Option<&str> = schema;
    let rows = client.query(sql, &[&schema_param])?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for row in &rows {
        let name: String = row.get(0);
        serde_json::to_writer(&mut out, &json!({ "name": name }))?;
        out.write_all(b"\n")?;
    }
    Ok(())
}

fn cmd_databases(client: &mut Client) -> Result<()> {
    let rows = client.query(
        "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname",
        &[],
    )?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for row in &rows {
        let name: String = row.get(0);
        serde_json::to_writer(&mut out, &json!({ "name": name }))?;
        out.write_all(b"\n")?;
    }
    Ok(())
}

fn cmd_schema(client: &mut Client, table: &str, schema: Option<&str>) -> Result<()> {
    let schema_param: Option<&str> = schema;
    let col_sql = "SELECT column_name, data_type, is_nullable, column_default, ordinal_position \
                   FROM information_schema.columns \
                   WHERE table_schema = COALESCE($1, current_schema()) \
                     AND table_name = $2 \
                   ORDER BY ordinal_position";
    let col_rows = client.query(col_sql, &[&schema_param, &table])?;
    let columns: Vec<Value> = col_rows.iter().map(row_to_json).collect();

    let idx_sql = "SELECT indexname, indexdef, indisunique, indisprimary \
                   FROM pg_indexes pi \
                   JOIN pg_class c ON c.relname = pi.indexname \
                   JOIN pg_index i ON i.indexrelid = c.oid \
                   WHERE pi.schemaname = COALESCE($1, current_schema()) \
                     AND pi.tablename = $2 \
                   ORDER BY indexname";
    let idx_rows = client.query(idx_sql, &[&schema_param, &table])?;
    let indexes: Vec<Value> = idx_rows.iter().map(row_to_json).collect();

    let out = json!({
        "table": table,
        "schema": schema,
        "columns": columns,
        "indexes": indexes,
    });
    emit_json(&out)
}

fn quote_ident(s: &str) -> String {
    let escaped = s.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

fn emit_json<T: serde::Serialize>(v: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut out, v)?;
    out.write_all(b"\n")?;
    Ok(())
}

/* ------------------------------------------------------------------------- */
/* serve mode — JSON-RPC over Unix socket                                    */
/* ------------------------------------------------------------------------- */

#[derive(serde::Deserialize)]
struct Req {
    id: Value,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(serde::Serialize)]
struct Resp {
    id: Value,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn cmd_serve(cli: &Cli, socket_path: &PathBuf) -> Result<()> {
    let _ = fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)
        .with_context(|| format!("binding {}", socket_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(socket_path)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(socket_path, perms)?;
    }
    eprintln!(
        "stryke-postgres-helper: listening on {}",
        socket_path.display()
    );

    let cfg = build_config(cli)?;

    for stream in listener.incoming() {
        let stream = stream?;
        if let Err(e) = serve_client(stream, &cfg) {
            eprintln!("stryke-postgres-helper: client closed with error: {e:#}");
        }
    }
    Ok(())
}

fn serve_client(stream: UnixStream, cfg: &Config) -> Result<()> {
    let reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);
    let mut client = cfg.connect(NoTls)?;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let req: Req = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                write_resp(
                    &mut writer,
                    &Resp {
                        id: Value::Null,
                        ok: false,
                        result: None,
                        error: Some(format!("parse error: {e}")),
                    },
                )?;
                continue;
            }
        };
        let resp = handle_rpc(&mut client, &req);
        write_resp(&mut writer, &resp)?;
        if req.method == "close" || req.method == "shutdown" {
            break;
        }
    }
    Ok(())
}

fn write_resp<W: Write>(w: &mut W, resp: &Resp) -> Result<()> {
    serde_json::to_writer(&mut *w, resp)?;
    w.write_all(b"\n")?;
    w.flush()?;
    Ok(())
}

fn handle_rpc(client: &mut Client, req: &Req) -> Resp {
    let result = match req.method.as_str() {
        "ping" => match client.query_one("SELECT 1", &[]) {
            Ok(_) => Ok(json!("ok")),
            Err(e) => return err_resp(&req.id, e.to_string()),
        },
        "query" => rpc_query(client, &req.params),
        "execute" => rpc_execute(client, &req.params),
        "close" | "shutdown" => Ok(json!("bye")),
        other => Err(anyhow!("unknown method `{other}`")),
    };
    match result {
        Ok(v) => Resp {
            id: req.id.clone(),
            ok: true,
            result: Some(v),
            error: None,
        },
        Err(e) => err_resp(&req.id, e.to_string()),
    }
}

fn err_resp(id: &Value, msg: String) -> Resp {
    Resp {
        id: id.clone(),
        ok: false,
        result: None,
        error: Some(msg),
    }
}

fn rpc_query(client: &mut Client, params: &Value) -> Result<Value> {
    let sql = params
        .get("sql")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("query: missing `sql`"))?;
    let bind_json = params.get("bind").cloned();
    let binds: Vec<BindVal> = match bind_json {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(arr)) => arr.into_iter().map(BindVal::from_json).collect(),
        _ => bail!("bind must be a JSON array"),
    };
    let refs = bind_refs(&binds);
    let rows = client.query(sql, &refs)?;
    let columns: Vec<String> = rows
        .first()
        .map(|r| r.columns().iter().map(|c| c.name().to_string()).collect())
        .unwrap_or_default();
    let rows_json: Vec<Value> = rows.iter().map(row_to_json).collect();
    Ok(json!({
        "columns": columns,
        "rows": rows_json,
    }))
}

fn rpc_execute(client: &mut Client, params: &Value) -> Result<Value> {
    let sql = params
        .get("sql")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("execute: missing `sql`"))?;
    let bind_json = params.get("bind").cloned();
    let binds: Vec<BindVal> = match bind_json {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(arr)) => arr.into_iter().map(BindVal::from_json).collect(),
        _ => bail!("bind must be a JSON array"),
    };
    let refs = bind_refs(&binds);
    let n = client.execute(sql, &refs)?;
    Ok(json!({ "affected_rows": n }))
}

/// Silence unused-import warnings when chrono types are pulled in but a
/// given build doesn't exercise every branch above.
#[allow(dead_code)]
fn _force_chrono_link() -> Option<NaiveDateTime> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── parse_bind ──────────────────────────────────────────────────

    #[test]
    fn parse_bind_none_empty() {
        assert!(parse_bind(None).unwrap().is_empty());
    }

    #[test]
    fn parse_bind_blank_string_empty() {
        assert!(parse_bind(Some("")).unwrap().is_empty());
        assert!(parse_bind(Some("   ")).unwrap().is_empty());
    }

    #[test]
    fn parse_bind_null_treated_as_empty() {
        assert!(parse_bind(Some("null")).unwrap().is_empty());
    }

    #[test]
    fn parse_bind_array_of_scalars() {
        let v = parse_bind(Some(r#"[1, "two", true, null, 3.5]"#)).unwrap();
        assert_eq!(v.len(), 5);
        assert!(matches!(v[0], BindVal::I64(1)));
        assert!(matches!(v[1], BindVal::Str(ref s) if s == "two"));
        assert!(matches!(v[2], BindVal::Bool(true)));
        assert!(matches!(v[3], BindVal::Null));
        match &v[4] {
            BindVal::F64(f) => assert_eq!(*f, 3.5),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn parse_bind_object_rejected_postgres_is_positional() {
        let err = parse_bind(Some(r#"{"a":1}"#)).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("array"));
        assert!(msg.contains("positional"));
    }

    #[test]
    fn parse_bind_invalid_json_errors() {
        let err = parse_bind(Some("{bad json}")).unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("parsing"));
    }

    // ─── BindVal::from_json ──────────────────────────────────────────

    #[test]
    fn bindval_from_json_null() {
        assert!(matches!(BindVal::from_json(Value::Null), BindVal::Null));
    }

    #[test]
    fn bindval_from_json_bool() {
        assert!(matches!(BindVal::from_json(json!(true)), BindVal::Bool(true)));
    }

    #[test]
    fn bindval_from_json_integer_is_i64() {
        assert!(matches!(BindVal::from_json(json!(42)), BindVal::I64(42)));
        assert!(matches!(BindVal::from_json(json!(-5)), BindVal::I64(-5)));
    }

    #[test]
    fn bindval_from_json_float_is_f64() {
        match BindVal::from_json(json!(2.5)) {
            BindVal::F64(f) => assert_eq!(f, 2.5),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn bindval_from_json_string_is_str() {
        match BindVal::from_json(json!("hi")) {
            BindVal::Str(s) => assert_eq!(s, "hi"),
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn bindval_from_json_array_is_json() {
        match BindVal::from_json(json!([1, 2, 3])) {
            BindVal::Json(v) => assert_eq!(v, json!([1, 2, 3])),
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn bindval_from_json_object_is_json() {
        match BindVal::from_json(json!({"k": 1})) {
            BindVal::Json(v) => assert_eq!(v, json!({"k": 1})),
            other => panic!("expected Json, got {other:?}"),
        }
    }

    // ─── bind_refs ───────────────────────────────────────────────────

    #[test]
    fn bind_refs_count_matches_input() {
        let v = vec![BindVal::I64(1), BindVal::Null, BindVal::Str("x".into())];
        assert_eq!(bind_refs(&v).len(), 3);
    }

    // ─── quote_ident ─────────────────────────────────────────────────

    #[test]
    fn quote_ident_wraps_in_double_quotes() {
        assert_eq!(quote_ident("users"), "\"users\"");
    }

    #[test]
    fn quote_ident_doubles_internal_double_quotes() {
        // SQL standard identifier escaping: " → ""
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn quote_ident_preserves_dots_spaces_unicode() {
        assert_eq!(quote_ident("my.schema"), "\"my.schema\"");
        assert_eq!(quote_ident("with space"), "\"with space\"");
        assert_eq!(quote_ident("日本語"), "\"日本語\"");
    }

    // ─── err_resp ────────────────────────────────────────────────────

    #[test]
    fn err_resp_marks_failure_and_carries_id_and_msg() {
        let r = err_resp(&json!(7), "boom".into());
        let s = serde_json::to_value(&r).unwrap();
        assert_eq!(s["id"], json!(7));
        assert_eq!(s["ok"], json!(false));
        assert_eq!(s["error"], json!("boom"));
        assert!(!s.as_object().unwrap().contains_key("result"));
    }

    #[test]
    fn err_resp_with_string_id_round_trips() {
        let r = err_resp(&json!("req-123"), "fail".into());
        let s = serde_json::to_value(&r).unwrap();
        assert_eq!(s["id"], json!("req-123"));
    }
}

#[allow(dead_code)]
fn _force_hashmap_link() -> HashMap<(), ()> {
    HashMap::new()
}
