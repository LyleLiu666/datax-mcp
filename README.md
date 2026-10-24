# DataX MCP

**Resumable, bounded DataX jobs through MCP.** Go manages durable jobs; Alibaba DataX's Java engine moves the rows. An independent worker continues when the MCP client disconnects.

[中文说明](docs/README.zh-CN.md) · [Correctness and limits](docs/design.md) · [Security](SECURITY.md)

Early v0.1: MySQL → PostgreSQL, single host, one worker per state directory. Full runs, append-only ID increments, pause/resume and transactional partition publication are implemented. This is not CDC or a replacement for a database migration assessment.

## Start here

Requires Linux or macOS, Go 1.26+, Java 8, and a DataX runtime with `mysqlreader`, `postgresqlwriter` and `streamreader`. PostgreSQL 17 and MySQL 8.4 were exercised locally. The pinned upstream JDBC driver uses MySQL native authentication; validate your server's auth/TLS compatibility before adopting it.

```sh
go build -o bin/datax-mcp ./cmd/datax-mcp
# Optional reproducible upstream build: Java 8, Maven, Git and Perl required.
./scripts/build-datax.sh
export DATAX_HOME="$PWD/.cache/datax"
export DATAX_MCP_STATE="$HOME/.local/state/datax-mcp"
./bin/datax-mcp doctor
```

The build pins upstream revision `80ec23d5c5328eb90ca364d2749e92dfaf44541e` and changes Maven packaging metadata only. No DataX source or binary is bundled in this repository. Set `JAVA_HOME`/`PATH` for Java 8, or `DATAX_JAVA` for the worker executable.

### 1. Prepare an approved profile

Create the destination table using [examples/target.sql](examples/target.sql). Review [examples/mysql-postgres.json](examples/mysql-postgres.json): source SQL must explicitly project the columns and constrain the integer key to `(lower, upper]`. Use an indexed key and a source account with SELECT-only privileges. `chunk_size` is ID-range width, not row count.

Provide `SOURCE_USER`, `SOURCE_PASSWORD`, `SOURCE_JDBC_URL` and `TARGET_PG_DSN` to the **worker's** environment through your secret manager. Example connection shapes (replace placeholders; do not commit credentials):

- Source: `jdbc:mysql://127.0.0.1:3306/example?useSSL=false&useUnicode=true&characterEncoding=UTF-8` — local test only; configure TLS for a network connection.
- Target: `postgresql://user:password@host:5432/database?sslmode=verify-full`.

Target URLs require an explicit `sslmode` (`disable`, `require`, `verify-ca`, `verify-full`) and a nonempty password. Only `sslrootcert`, `sslcert`, and `sslkey` are accepted as additional URL options; test certificate-format compatibility with both JDBC and pgx. Use a dedicated destination role/database; the role needs schema/table creation and target SELECT/INSERT/UPDATE privileges. Target tables must have a single integer UNIQUE/PRIMARY KEY, projected columns with compatible types, and no externally modifying writers.

### 2. Start the independent worker

```sh
./bin/datax-mcp worker
```

Run this under systemd, launchd, a container supervisor, or a dedicated terminal. It handles one partition at a time, parks failures for explicit resume, and releases its lock when it exits. `worker --once` processes at most one partition. Keep the worker and MCP server on the same host and state directory; never use network storage for SQLite state.

### 3. Connect an MCP client

Adapt [examples/mcp.json](examples/mcp.json) to absolute paths. Point the client at `datax-mcp serve --state /absolute/private/state`. The stdio server does not need database passwords. It writes only MCP protocol messages to stdout.

| Tool | Purpose |
| --- | --- |
| `sync_capabilities` | Inspect worker/runtime readiness and limitations |
| `sync_register` | Register a reviewed native DataX reader and target mapping |
| `sync_profiles` | List profiles with unresolved environment references |
| `sync_start` | Queue `full` or `incremental`, returning a run ID immediately |
| `sync_status` | Read partition progress and stage-to-target verification counts |
| `sync_pause` | Stop scheduling after the current partition |
| `sync_resume` | Continue a paused/failed run from durable state |

Start with a small full range and inspect the resulting data. Supply a fixed `upper` observed in the source; `full` also accepts an exclusive `lower` (default 0). An incremental run starts at the last completed run's upper bound. Profiles are immutable; changed mappings require a new name. MCP registration is a privileged operator interface, not an SQL sandbox.

Supervised applications can also invoke the actual MCP protocol through the bundled client:

```sh
printf '%s' '{}' | ./bin/datax-mcp call --tool sync_capabilities
```

`call` starts a stdio MCP session using the official SDK, reads JSON arguments from stdin, and prints the MCP result envelope. It does not shortcut through the CLI state methods. Integer cursors are decoded directly as int64 to avoid floating-point rounding in default application.

The administrative CLI calls the same control logic:

```sh
./bin/datax-mcp register --file examples/mysql-postgres.json
./bin/datax-mcp start --profile sample-items --mode full --lower 0 --upper 1000
./bin/datax-mcp status
./bin/datax-mcp pause --run RUN_ID
./bin/datax-mcp resume --run RUN_ID
./bin/datax-mcp start --profile sample-items --mode incremental --upper 2000
```

## What resume guarantees

Each attempt writes to a unique staging table. After DataX succeeds, PostgreSQL validates key uniqueness/range, merges the selected fields, compares stage and target, writes a commit receipt, and drops staging in one transaction. Only then does SQLite advance. If the worker dies between the PostgreSQL commit and the SQLite update, the receipt recovers the checkpoint without rerunning DataX.

Completed partitions remain visible even while later partitions are pending. This is partition-atomic publication, not a whole-run snapshot. `verified_rows` proves equality between staging and the destination, **not an independent source-to-destination checksum**. Source fidelity requires a verified projection and pilot.

Limits: no CDC, automatic deletion propagation, multi-table atomicity, mutable-key support, cluster scheduling, or negative/zero key support. Append increments do not capture edits, late commits below the watermark, or deletes. An explicit full rerun refreshes existing selected fields but does not delete absent rows. Do not claim exactness for untested types; use explicit string projections for timestamps/decimals when precision matters, and `bytea` for arbitrary bytes including NUL. See [design](docs/design.md) for restart and backup requirements.

## Verification

```sh
go test -race ./...
go vet ./...
# Requires Docker; starts disposable, isolated synthetic databases.
./scripts/test-integration.sh
```

Tests exercise the actual MCP stdio protocol and client disconnect, DataX Java execution, MySQL NULL/empty/Unicode/NUL/large-ID/precision fixtures, pause/failure/resume, and the destination-commit-before-local-checkpoint crash boundary. Integration tests skip unless their explicit test environment is configured; unit-test green does not imply DataX integration passed.

Apache-2.0. Independent project; not affiliated with Alibaba or the MCP maintainers.
