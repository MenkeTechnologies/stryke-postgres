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
val @rows = Postgres::query "SELECT id, name FROM users WHERE created_at > \$1",
                           bind => ["2025-01-01"]
@rows |> ep

# Streaming variant — no full-result buffering.
Postgres::query_stream "SELECT * FROM big_table",
    callback => fn ($row) { process $row }

# Write paths return { affected }.
val $r = Postgres::execute "UPDATE users SET name = \$1 WHERE id = \$2",
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
val @rows = Postgres::insert_many "users",
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
val @r = Postgres::upsert "kv", { id => 2, name => "b" },
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
Postgres::savepoint         $name, %opts → 1       # SAVEPOINT <name>
Postgres::release_savepoint $name, %opts → 1       # RELEASE SAVEPOINT <name>
Postgres::rollback_to       $name, %opts → 1       # ROLLBACK TO SAVEPOINT <name>
```

Savepoints are nested rollback points inside an open transaction. The name
is identifier-quoted (it can't be a bound parameter). `rollback_to` undoes
work back to the savepoint while leaving the outer transaction open.

```stryke
Postgres::begin %c
Postgres::execute "INSERT INTO t VALUES (1)", %c
Postgres::savepoint "a", %c
Postgres::execute "INSERT INTO t VALUES (2)", %c
Postgres::rollback_to "a", %c    # row (2) undone, row (1) kept
Postgres::commit %c
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
Postgres::activity     %opts → @{ {pid, user, state, wait_event_type, age_seconds, query} }   # pg_stat_activity
Postgres::locks        %opts → @{ {pid, locktype, mode, granted, relation} }
Postgres::sequences    %opts → @names
Postgres::extensions   %opts → @{ {name, version} }
Postgres::triggers     %opts → @{ {name, table, event, timing} }
Postgres::cancel_backend $pid, %opts → { pid, ok }           # opt: terminate => 1 (hard kill)
```

### Schema discovery

```stryke
Postgres::describe        $sql, %opts → { params, columns }   # prepare-only; type-checks without running
Postgres::current         %opts → { database, user, schema, pid }   # this connection's identity
Postgres::schemas         %opts → @names                      # non-system schemas (namespaces)
Postgres::matviews        %opts → @names                      # materialized views as schema.matview
Postgres::server_settings %opts → @{ {name, setting, description} }   # pg_settings (SHOW ALL)
Postgres::server_encoding %opts → { server_encoding, client_encoding, timezone }
Postgres::constraints     $table, %opts → @{ {name, type, definition} }   # type: primary key/foreign key/unique/check/exclusion
Postgres::foreign_keys    $table, %opts → @{ {name, column, references_table, references_column} }
Postgres::primary_key     $table, %opts → @columns            # PK column names in key order; empty when none
Postgres::column_defaults $table, %opts → @{ {column, default} }   # only columns with a default
Postgres::table_stats     $table, %opts → { table, live_tuples, dead_tuples, seq_scan, idx_scan, n_tup_ins, n_tup_upd, n_tup_del }
```

`describe` prepares the statement via the extended protocol and reports the
bind-parameter types (`$1`, `$2`, … in order) and the result-column shape
WITHOUT executing it — the analyze-only counterpart of `explain`.
`constraints`/`foreign_keys`/`primary_key`/`column_defaults` complement
`schema` (which reports name/type/nullable). The `$table` arg is bound via
`$1::regclass`, so a bare or schema-qualified name resolves safely.

Pure helpers — connection-string and quoting utilities that open no socket:

```stryke
Postgres::parse_dsn($dsn)      → { scheme, user, password, host, port, dbname, params }
Postgres::parse_keyword_dsn($dsn) → { user, password, host, port, dbname, params }   # libpq keyword/value form (host=… dbname=…); space-separated, single-quoted values, \' \\ escapes
Postgres::build_keyword_dsn(%opts) → $dsn   # parts → libpq keyword/value DSN; inverse of parse_keyword_dsn (well-known keys first, params sorted, spaces single-quoted)
Postgres::build_dsn(%opts)     → $dsn        # parts → URI DSN; inverse of parse_dsn
Postgres::quote_ident($name)   → $quoted     # "weird""col"
Postgres::unquote_ident($quoted) → $name     # inverse of quote_ident: strip quotes, un-double
Postgres::quote_qualified_ident($name) → $quoted  # public.my table → "public"."my table"
Postgres::parse_qualified_ident($name) → \@parts  # "public"."my table" → ["public","my table"]; inverse of quote_qualified_ident
Postgres::quote_literal($val)  → $quoted     # 'O''Brien'
Postgres::escape_like($val)    → $escaped    # backslash-escape LIKE wildcards: 100% → 100\%, a_b → a\_b, \ → \\
Postgres::unescape_like($pat)  → $literal    # inverse: recover the literal (100\% → 100%); rejects an unescaped wildcard or dangling backslash
Postgres::quote_nullable($val) → $quoted     # like quote_literal, but undef → NULL (unquoted)
Postgres::unquote_literal($lit) → $val       # 'O''Brien' → O'Brien; inverse of quote_literal (standard mode)
Postgres::dollar_quote($val, $tag?) → $quoted  # $tag$val$tag$ (no escaping); auto-picks a non-colliding tag ($$..$$ or $dq0$, …)
Postgres::unquote_dollar($quoted) → $val      # inverse of dollar_quote: $tag$val$tag$ → val (content verbatim)
Postgres::format_array(\@elems) → $literal    # ["a,b","c"] → {"a,b",c} (Postgres array input syntax)
Postgres::format_in_list(\@values) → $operand # [1,"a",undef] → (1, 'a', NULL) for `col IN (...)`; empty → (NULL); mixed types
Postgres::parse_in_list($list)     → \@values  # inverse: (1, 'a', NULL, TRUE) → [1,"a",undef,1]; '' un-doubles; commas in quotes don't split
Postgres::parse_array($literal) → \@elems     # {"a,b",NULL,c} → ["a,b",undef,"c"]; inverse of format_array (1-D)
Postgres::parse_range($literal) → \%{ empty, lower, upper, lower_inclusive, upper_inclusive }  # [3,7) → 3 incl .. 7 excl; (,5] → unbounded .. 5 incl; "empty" range; omitted bound = undef
Postgres::split_statements($sql) → \@statements  # split a SQL script on top-level `;` (respects strings, dollar-quotes, line/block comments); blank statements dropped
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
    crud.stk
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
