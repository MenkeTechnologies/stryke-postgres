```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                   [ p o s t g r e s ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-postgres/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-postgres/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[POSTGRESQL CLIENT FOR STRYKE // OPT-IN PACKAGE]`

> *"Postgres without the ORM."*

PostgreSQL client for stryke. Opt-in package, kept out of the stryke core
binary so the daily-driver install stays slim.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-mysql`](https://github.com/MenkeTechnologies/stryke-mysql) · [`stryke-mongo`](https://github.com/MenkeTechnologies/stryke-mongo) · [`stryke-duckdb`](https://github.com/MenkeTechnologies/stryke-duckdb) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] Why this is a package, not a builtin](#0x00-why-this-is-a-package-not-a-builtin)
- [\[0x01\] Install](#0x01-install)
- [\[0x02\] Quick start](#0x02-quick-start)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x06\] Type encoding](#0x06-type-encoding)
- [\[0x07\] Bind parameters](#0x07-bind-parameters)
- [\[0x08\] Tests](#0x08-tests)
- [\[0x09\] Dev workflow](#0x09-dev-workflow)
- [\[0x0A\] Layout](#0x0a-layout)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] Why this is a package, not a builtin

Same rationale as [stryke-arrow](../stryke-arrow) and
[stryke-mysql](../stryke-mysql): a real database client pulls in 100+
transitive crates (TLS, async runtime, type encoders). Most stryke
one-liners never touch Postgres; for the ones that do, opt in with this
package.

`stryke-postgres` ships as a thin stryke library plus a Rust cdylib
(`libstryke_postgres.{dylib,so}`) built from this repo. The cdylib is
dlopened in-process on first `use Postgres`; each `Postgres::*` wrapper
calls a `pg__*` export with a JSON args dict and decodes the JSON reply.

## [0x01] Install

From a release (no rustc on the consumer machine):

```sh
s pkg install -g github.com/MenkeTechnologies/stryke-postgres
```

From a local checkout:

```sh
cd ~/projects/stryke-postgres
cargo build --release          # produces target/release/libstryke_postgres.{dylib,so}
s pkg install -g .             # cdylib lands in ~/.stryke/store/postgres@<version>/
```

Or:

```sh
make install
```

The cdylib is dlopened in-process on first `use Postgres`. A
`postgres::Client` cache keyed by connection URL is held in `OnceCell`
— no fork-per-call, no fresh TCP+TLS+auth handshake. Honors
`DATABASE_URL` and `POSTGRES_URL` env vars.

## [0x02] Quick start

```stryke
use Postgres

# Set $DATABASE_URL once, omit the named arg everywhere.
$ENV{DATABASE_URL} = "postgres://wizard@127.0.0.1:5432/app"

# Single scalar.
p Postgres::query_scalar "SELECT COUNT(*) FROM users"

# Rows with positional placeholders ($1, $2, …).
my @rows = Postgres::query "SELECT id, name FROM users WHERE created_at > \$1",
                           bind => ["2025-01-01"]
@rows |> ep

# Streaming variant — no full-result buffering.
Postgres::query_stream "SELECT * FROM big_table",
    callback => sub ($row) { process $row }

# Write paths return { affected }.
my $r = Postgres::execute "UPDATE users SET name = \$1 WHERE id = \$2",
                          bind => ["alice", 42]
p "updated $r->{affected}"

# Bulk insert (array of hashes; columns inferred from first row's keys).
# Returns the inserted-row count.
Postgres::insert_many "users",
    [{ name => "x", score => 1, meta => { color => "blue" } },
     { name => "y", score => 2, meta => { color => "red"  } }]

# With RETURNING — get the generated rows back instead of a count. The
# returning clause is a comma-separated identifier list (or "*"); each
# token is validated, so it's injection-safe.
my @rows = Postgres::insert_many "users",
    [{ name => "z", score => 3 }],
    returning => "id"
p $rows[0]->{id}

# Schema introspection.
p to_json Postgres::schema "users"
p Postgres::tables |> ep
```

Connection sources (priority order):

1. `url => "postgres://user:pass@host:port/db"` named arg
2. Individual named args: `host`, `port`, `user`, `password`, `database`
3. `$ENV{DATABASE_URL}` (or `$ENV{POSTGRES_URL}`) when neither `url` nor
   `host` is given

## [0x04] API reference

### Read paths

```stryke
Postgres::query        $sql, %opts → @rows
Postgres::query_stream $sql, %opts → $count
Postgres::query_one    $sql, %opts → \%row | undef
Postgres::query_col    $sql, %opts → @values
Postgres::query_scalar $sql, %opts → $value | undef
Postgres::dump         $table, %opts → @rows
```

`%opts` keys: `url`, `host`, `port`, `user`, `password`, `database`,
`bind`, `limit` (dump only), `callback` (stream only). `bind` is an
arrayref (positional `$1`, `$2`, …).

### Write paths

```stryke
Postgres::execute     $sql, %opts → { affected }
Postgres::exec_file   $path, %opts → per-script result
Postgres::insert_many $table, $rows_aref, %opts → $inserted_count | @rows  # @rows when opts{returning} set
Postgres::upsert      $table, $row_href, %opts → $affected | @rows         # INSERT … ON CONFLICT DO UPDATE
Postgres::update      $table, $set_href, $where?, %opts → $affected   # UPDATE … SET … [WHERE]
Postgres::delete      $table, $where?, %opts → $affected               # DELETE FROM … [WHERE]
Postgres::truncate    $table, %opts → 1            # %opts restart_identity => 1 for RESTART IDENTITY
```

### Bulk transfer (COPY)

```stryke
Postgres::copy_in   $sql, $data, %opts → $row_count   # COPY … FROM STDIN — bulk load
Postgres::copy_out  $sql, %opts → $data               # COPY … TO STDOUT — bulk export
```

`copy_in` streams `$data` (text/CSV/TSV lines, per the COPY statement's
FORMAT) straight into the table — Postgres's fastest bulk-load path.
`copy_out` returns the server's COPY payload as a string.

### LISTEN / NOTIFY

```stryke
Postgres::notify  $channel, %opts → { ok }            # %opts payload => "..."
Postgres::listen  $channels, %opts → @notifications   # name or aref; %opts timeout_ms (default 1000)
```

`notify` sends via `pg_notify($1,$2)` (bound, not interpolated). `listen`
issues `LISTEN` on each channel (identifier-quoted) and drains pending
notifications for `timeout_ms`, returning `{ channel, payload, pid }` rows.

`update` and `delete` complete the CRUD surface. `update` binds the `$set`
values (`SET col = $1, …`) and interpolates `$where` (pass trusted values
— a parameterized condition would collide with the SET placeholders, so
use `execute` for that). `delete` interpolates `$where`. Both omit `$where`
to affect every row and return the affected-row count. Table and SET
column names are identifier-validated.

```stryke
Postgres::update "users", { status => "active", seen => 1 }, "id = 42"
Postgres::delete "sessions", "expired_at < now()"
```

`upsert` inserts a single row and, on a unique/PK conflict over the
`conflict` columns, updates the `update` columns from the proposed row
(`EXCLUDED.*`). Options: `conflict => \@cols` (required); `update =>
\@cols` (defaults to every row column that isn't a conflict target — an
empty list makes the conflict a no-op, `DO NOTHING`); `returning =>
"col,…" | "*"` to get the affected rows back instead of a count. Column,
conflict, and update names are identifier-validated; values are bound.

```stryke
# insert, or on id-conflict overwrite name + hits
Postgres::upsert "kv", { id => 1, name => "a", hits => 1 }, conflict => ["id"]
# only bump hits on conflict, keep the existing name
Postgres::upsert "kv", { id => 1, name => "x", hits => 9 },
                 conflict => ["id"], update => ["hits"]
# get the row back
my @r = Postgres::upsert "kv", { id => 2, name => "b" },
                         conflict => ["id"], returning => "*"
```

### Transactions

All statements issued with the same `%opts` run on the same cached
backend connection, so these ride on connection affinity (no extra FFI).

```stryke
Postgres::begin       %opts → 1                    # BEGIN
Postgres::commit      %opts → 1                    # COMMIT
Postgres::rollback    %opts → 1                    # ROLLBACK
Postgres::transaction $code, %opts → $code_result  # BEGIN; $code->(); COMMIT — or ROLLBACK + re-raise on die
```

```stryke
Postgres::transaction fn {
    Postgres::execute "INSERT INTO accounts (id, cents) VALUES (1, 100)", %c
    Postgres::execute "UPDATE accounts SET cents = cents - 100 WHERE id = 2", %c
}, %c
```

### Metadata

```stryke
Postgres::ping         %opts → 1 | 0
Postgres::tables       %opts → @names
Postgres::databases    %opts → @names
Postgres::schema       $table, %opts → column metadata for $table
Postgres::count        $table, $where?, %opts → $row_count    # SELECT count(*) [WHERE $where]
Postgres::exists       $table, $where?, %opts → 1 | 0         # SELECT EXISTS(…) — short-circuits
Postgres::table_exists $name, %opts → 1 | 0                   # $name must be a plain identifier
Postgres::views        %opts → @names                        # user views as schema.view
Postgres::functions    %opts → @names                        # user functions as schema.fn
Postgres::indexes      %opts → @{ {name, def} }              # opt: table => "t"
Postgres::roles        %opts → @{ {name, superuser, can_login} }
Postgres::explain      $sql, %opts → @plan_lines             # opt: analyze => 1, params
Postgres::db_size      %opts → { bytes, pretty }             # current database size
Postgres::table_size   $table, %opts → { table, bytes, pretty }
Postgres::activity     %opts → @{ {pid, user, state, age_seconds, query} }   # pg_stat_activity
Postgres::locks        %opts → @{ {pid, locktype, mode, granted, relation} }
Postgres::sequences    %opts → @names
Postgres::extensions   %opts → @{ {name, version} }
Postgres::triggers     %opts → @{ {name, table, event, timing} }
Postgres::cancel_backend $pid, %opts → { pid, ok }           # opt: terminate => 1 (hard kill)
```

Pure helpers — connection-string and quoting utilities that open no socket:

```stryke
Postgres::parse_dsn($dsn)      → { scheme, user, password, host, port, dbname, params }
Postgres::build_dsn(%opts)     → $dsn        # parts → URI DSN; inverse of parse_dsn
Postgres::quote_ident($name)   → $quoted     # "weird""col"
Postgres::quote_qualified_ident($name) → $quoted  # public.my table → "public"."my table"
Postgres::quote_literal($val)  → $quoted     # 'O''Brien'
Postgres::format_array(\@elems) → $literal    # ["a,b","c"] → {"a,b",c} (Postgres array input syntax)
```

`count` and `exists` interpolate the table name and `$where`; pass binds
in `%opts{bind}` (e.g. `exists "t", "id = $1", bind => [42]`). `exists`
uses SQL `EXISTS`, which stops at the first matching row — prefer it over
`count(…) > 0` when you only need a yes/no.

### Versions

```stryke
Postgres::version()              → package version string
Postgres::server_version(%opts)  → Postgres `version()` build string
```

## [0x06] Type encoding

PostgreSQL → JSON encoding:

| Postgres | JSON | Notes |
|---|---|---|
| `bool` | bool | |
| `int2`, `int4`, `int8` | number | |
| `float4`, `float8` | number | |
| `text`, `varchar`, `bpchar`, `name` | string | |
| `date` | `"YYYY-MM-DD"` | |
| `timestamp` | `"YYYY-MM-DD HH:MM:SS[.ffffff]"` | |
| `timestamptz` | RFC 3339 | |
| `uuid` | string | |
| `json`, `jsonb` | preserved as JSON | |
| other | string | text fallback; null when no text conversion exists |
| `NULL` | null | |

## [0x07] Bind parameters

PostgreSQL placeholders are positional `$1`, `$2`, …  Pass binds as a JSON
array (stryke side: an arrayref). Add explicit casts (`$1::int`, `$2::text`)
when type inference is ambiguous — Postgres is stricter than MySQL here.

```stryke
Postgres::query  "SELECT * FROM t WHERE id = \$1::int",  bind => [42]
Postgres::query  'SELECT $1::text || $2::text AS r',     bind => ["a", "b"]
Postgres::execute 'INSERT INTO t (data) VALUES ($1)',    bind => [{x => 1}]   # jsonb
```

The cdylib's `json_to_param` maps JSON binds to wire values: null → NULL,
bool → bool, int → `i64`, float → `f64`, string → text, array/object →
jsonb. Add explicit casts for expressions where Postgres can't infer the
type from context.

## [0x08] Tests

```sh
cargo test                                       # unit + contract tests, no live calls
DATABASE_URL='postgres://…' s test t/            # end-to-end against live Postgres
```

The end-to-end suite skips cleanly when none of `$DATABASE_URL`,
`$POSTGRES_URL`, `$POSTGRES_DSN` points at a reachable server.

## [0x09] Dev workflow

```sh
make             # release build
make test        # cargo test + s test t/
make install     # release + pkg install -g .
make clean
```

## [0x0A] Layout

```
stryke-postgres/
  stryke.toml                    # stryke package manifest ([ffi] table)
  Cargo.toml                     # cdylib crate manifest
  Makefile
  src/lib.rs                     # cdylib — pg__* extern "C" exports + client cache
  lib/
    Postgres.stk                 # `use Postgres`
  t/
    test_postgres.stk            # end-to-end (gated on $DATABASE_URL)
    test_stryke_postgres_surface.stk
  tests/                         # Rust contract test + repo lint gates
  examples/
    quick_query.stk
    bulk_load.stk
    discover.stk
    dump_table.stk
    explain.stk
  .github/workflows/
    ci.yml                       # cargo check/test + live-pg smoke
    release.yml                  # cross-compile + GH release on tag push
```

## [0xFF] License

MIT.
