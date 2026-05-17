# stryke-postgres

PostgreSQL client for stryke. Opt-in package, kept out of the stryke core
binary so the daily-driver install stays slim.

Created by MenkeTechnologies.

## Why this is a package, not a builtin

Same rationale as [stryke-arrow](../stryke-arrow) and
[stryke-mysql](../stryke-mysql): a real database client pulls in 100+
transitive crates (TLS, async runtime, type encoders). Most stryke
one-liners never touch Postgres; for the ones that do, opt in with this
package.

`stryke-postgres` ships as a thin stryke library plus a Rust helper binary
(`stryke-postgres-helper`) built from this repo. The stryke side spawns the
helper per call and parses NDJSON over a pipe.

## Install

```sh
cd ~/projects/stryke-postgres
cargo build --release          # produces target/release/stryke-postgres-helper
s pkg install -g .             # installs `postgres` and `postgres-build` CLIs
```

Or:

```sh
make install
```

## Quick start

```stryke
use Postgres

# Set $POSTGRES_DSN once, omit the named arg everywhere.
$ENV{POSTGRES_DSN} = "postgres://wizard@127.0.0.1:5432/app"

# Single scalar.
p Postgres::query_scalar "SELECT COUNT(*) FROM users"

# Rows with positional placeholders ($1, $2, …).
my @rows = Postgres::query "SELECT id, name FROM users WHERE created_at > \$1",
                           bind => ["2025-01-01"]
@rows |> ep

# Streaming variant — no full-result buffering.
Postgres::query_stream "SELECT * FROM big_table",
    callback => sub ($row) { process $row }

# Write paths return { affected_rows }.
my $r = Postgres::execute "UPDATE users SET name = \$1 WHERE id = \$2",
                          bind => ["alice", 42]
p "updated $r->{affected_rows}"

# Bulk insert (array of hashes; columns inferred from first row's keys).
Postgres::insert_many "users",
    [{ name => "x", score => 1, meta => { color => "blue" } },
     { name => "y", score => 2, meta => { color => "red"  } }]

# RETURNING for generated IDs.
my @rows = Postgres::insert_many "users",
    [{ name => "z" }],
    returning => "id, name"

# Schema introspection.
p to_json Postgres::schema "users"
p Postgres::tables |> ep
```

DSN sources (priority order):

1. `dsn => "postgres://user:pass@host:port/db"` named arg
2. `$ENV{POSTGRES_DSN}` (or `$ENV{DATABASE_URL}`)
3. Individual flags: `host`, `port`, `user`, `password`, `database`

## CLI: `postgres`

```sh
postgres query   'SELECT * FROM users WHERE id = $1' --bind='[42]'
postgres execute 'UPDATE users SET active = true WHERE id = $1' --bind='[42]'
postgres exec   --file=migrate.sql
postgres dump   --table=users --where='active = true' --order-by=id --limit=100
postgres tables [--schema=myschema]
postgres databases
postgres schema --table=users [--schema=myschema]
postgres ping
postgres build                              # `cargo build --release`
postgres version
```

Connection flags (also accept env vars):

```
--dsn URL              $POSTGRES_DSN
--database-url URL     $DATABASE_URL    (libpq convention; equivalent)
--host H               $POSTGRES_HOST
--port P               $POSTGRES_PORT
--user U               $POSTGRES_USER
--password PW          $POSTGRES_PASSWORD
--database D           $POSTGRES_DATABASE
--application-name N   (default: stryke-postgres-helper)
--connect-timeout SECONDS
```

## API reference

### Read paths

```stryke
Postgres::query        $sql, %opts → @rows
Postgres::query_stream $sql, %opts → $count
Postgres::query_one    $sql, %opts → \%row | undef
Postgres::query_col    $sql, %opts → @values
Postgres::query_scalar $sql, %opts → $value | undef
Postgres::dump         $table, %opts → @rows
```

`%opts` keys: `dsn`, `host`, `port`, `user`, `password`, `database`,
`application_name`, `connect_timeout`, `bind`, `columnar`, `with_meta`,
`limit`, `callback` (stream only). `bind` is an arrayref (positional `$1`,
`$2`, …).

### Write paths

```stryke
Postgres::execute     $sql, %opts → { affected_rows }
Postgres::exec_file   $path, %opts → [{ affected_rows }, ...]
Postgres::insert_many $table, $rows_aref, %opts → { affected_rows }
                                                 | @rows (when returning => "...")
```

`insert_many` accepts `returning => "id, name, …"` to fetch generated
columns. Without it you get an `{affected_rows}` hash; with it you get the
row list.

### Metadata

```stryke
Postgres::ping       %opts → 1 | ""
Postgres::tables     %opts → @names           # opts.schema overrides current_schema()
Postgres::databases  %opts → @names
Postgres::schema     $table, %opts → { table, schema, columns => [...], indexes => [...] }
```

### Helper plumbing

```stryke
Postgres::helper_path()   → $abs_path
Postgres::ensure_built()  → $abs_path     # cargo-builds if missing
Postgres::version()       → "stryke-postgres-helper 0.1.0"
```

## Helper protocol

```sh
stryke-postgres-helper --dsn 'postgres://…' query 'SELECT * FROM t WHERE id = $1' --bind '[42]'
stryke-postgres-helper --dsn 'postgres://…' execute 'UPDATE …' --bind '["x", 1]'
stryke-postgres-helper --dsn 'postgres://…' exec --file migrate.sql
stryke-postgres-helper --dsn 'postgres://…' schema --table users
stryke-postgres-helper --dsn 'postgres://…' ping
```

Output:

* `query` → NDJSON rows on stdout. `--columnar` emits one `{columns, rows}`
  object. `--with-meta` prepends a `{"meta":{columns:[...]}}` line.
* `execute` → `{affected_rows}`
* `exec` → array of per-statement objects
* `schema` → `{table, schema, columns:[...], indexes:[...]}`
* `tables`, `databases` → NDJSON `{"name": ...}`
* `ping` → `ok` on stdout, exit 0; non-zero on failure

### Persistent serve mode (experimental)

```sh
stryke-postgres-helper --dsn 'postgres://…' serve --socket-path /tmp/sp.sock &
```

JSON-RPC over a Unix socket: each line is `{"id":N,"method":"query|execute|ping|close","params":{...}}`.
The connection is reused across requests. The stryke side's persistent-connect
API will pick this up once stryke gains a Unix-socket client builtin.

## Type encoding

PostgreSQL → JSON encoding:

| Postgres | JSON | Notes |
|---|---|---|
| `bool` | bool | |
| `int2`, `int4`, `int8` | number | |
| `oid` | number | |
| `float4`, `float8` | number | |
| `numeric` | string | text-cast to preserve precision |
| `text`, `varchar`, `bpchar`, `name`, `char` | string | |
| `bytea` | string | UTF-8 if valid; otherwise `"base64:…"` |
| `date` | `"YYYY-MM-DD"` | |
| `time` | `"HH:MM:SS.ffffff"` | |
| `timestamp` | `"YYYY-MM-DD HH:MM:SS.ffffff"` | |
| `timestamptz` | RFC 3339 | |
| `uuid` | string | |
| `json`, `jsonb` | preserved as JSON | |
| array types (`int4[]`, `text[]`, …) | array | element-wise |
| other | string | falls back to text representation |
| `NULL` | null | |

## Bind parameters

PostgreSQL placeholders are positional `$1`, `$2`, …  Pass binds as a JSON
array (stryke side: an arrayref). Add explicit casts (`$1::int`, `$2::text`)
when type inference is ambiguous — Postgres is stricter than MySQL here.

```stryke
Postgres::query  "SELECT * FROM t WHERE id = \$1::int",  bind => [42]
Postgres::query  'SELECT $1::text || $2::text AS r',     bind => ["a", "b"]
Postgres::execute 'INSERT INTO t (data) VALUES ($1)',    bind => [{x => 1}]   # jsonb
```

The helper's `BindVal` narrows JSON ints to whatever the inferred column
type expects (int2/int4/int8/float4/float8/numeric/text) so casts aren't
strictly required for INSERT into a typed column — only for expressions
where Postgres can't infer.

## Tests

```sh
cargo test                                       # unit tests (scaffold)
POSTGRES_DSN='postgres://…' s test t/            # end-to-end against live Postgres
```

The end-to-end suite skips cleanly when `$POSTGRES_DSN` is unset or the
helper isn't built.

## Dev workflow

```sh
make             # release build
make debug       # faster compile
make test        # cargo test + s test t/
make install     # release + pkg install -g .
make clean
```

## Layout

```
stryke-postgres/
  stryke.toml                    # stryke package manifest
  Cargo.toml                     # Rust helper crate manifest
  Makefile
  src/main.rs                    # stryke-postgres-helper binary
  lib/
    Postgres.stk                 # `use Postgres`
  bin/
    postgres.stk                 # `postgres` CLI
    postgres-build.stk
  t/
    test_postgres.stk            # end-to-end (gated on $POSTGRES_DSN)
  examples/
    quick_query.stk
    bulk_load.stk
    dump_table.stk
  .github/workflows/
    ci.yml                       # cargo check/test + live-pg smoke
    release.yml                  # cross-compile + GH release on tag push
```

## License

MIT.
