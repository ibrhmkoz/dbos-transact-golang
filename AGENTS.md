# Repository Notes

## Test Results

Run tests with:

```bash
just test
```

The recipe records the complete `go test -json` stream in DuckDB.

Each run gets an immutable database:

```text
.test-results/runs/<run-id>.duckdb
```

`.test-results/latest.duckdb` is a symlink to the newest run. Query it with:

```bash
just results
```

Do not replace this with one shared writable DuckDB file. DuckDB permits only
one read-write process, and long-lived GoLand connections cause lock failures.
The per-run files allow new recordings while older files remain open.

Relevant scripts:

- `scripts/record-tests.sh`: captures output and creates one DuckDB per run.
- `scripts/load-test-results.sql`: loads structured Go test events.
- `scripts/test-results-schema.sql`: tables and reusable views.
- `scripts/test-results.sql`: standard analysis reports.

Useful views:

- `latest_test_run`: latest run metadata and wall time.
- `latest_test_results`: every test and subtest result.
- `latest_leaf_test_results`: excludes parent tests whose elapsed time includes
  subtests. Use this view when summing test durations.
- `latest_test_events`: ordered raw Go test events and output.
- `latest_test_parallelism`: serial/parallel leaf-test counts and work.
- `latest_parallel_phase`: wall time from first parallel `cont` event to final
  test completion.
- `failed_test_results`: failures across recorded runs in the current file.

Tests are marked parallel when the test or an ancestor emitted Go's `pause`
event from `t.Parallel()`. Nested subtests inherit their parent's parallel
classification.

Important timing interpretation:

- `serial_leaf_seconds`: serial work; directly contributes to the serial phase.
- `parallel_work_seconds`: summed parallel test work; not wall time.
- `parallel_phase_wall_seconds`: actual wall time consumed by the parallel
  phase.
- `effective_parallelism`: total leaf work divided by total wall time.
- Go pauses parallel top-level tests until all serial top-level tests finish.

Find the next tests to parallelize:

```sql
SELECT test, elapsed_seconds
FROM latest_leaf_test_results
WHERE NOT parallel
ORDER BY elapsed_seconds DESC
LIMIT 30;
```

Inspect one test's complete output:

```bash
TEST_NAME='TestName/Subtest' \
  duckdb -readonly .test-results/latest.duckdb -f scripts/test-results.sql
```

## Parallel Tests

Use `parallelTest(t)` instead of calling `t.Parallel()` directly. It also
prevents process-wide leak checks from reporting goroutines belonging to other
parallel tests.

Test database isolation:

- SQLite uses one temporary database file per test.
- PostgreSQL uses one migrated template database and clones one database per
  test.
- CockroachDB creates one isolated database per test without templates.

`just test postgres` starts an isolated `postgres:16-alpine` Testcontainer,
builds the migrated template, runs tests, then terminates the container. It
does not use PostgreSQL instances already running on the host.

Set `DBOS_SYSTEM_DATABASE_URL` only to override Testcontainers, such as for
CockroachDB CI or an externally managed PostgreSQL server. The configured user
must be able to create and drop databases.

```text
DBOS_SYSTEM_DATABASE_URL=postgresql://<user>@localhost:5432/dbos?sslmode=disable
```

Keep tests serial when they depend on process-wide state that has not been
isolated, including:

- Environment mutation through `t.Setenv`, `os.Setenv`, or `os.Unsetenv`.
- Fixed admin-server port `3001`.
- Shared mutable package-level test variables.
- Process-wide goroutine leak assertions.
