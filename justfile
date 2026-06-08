set dotenv-load := true

default:
    @just --list

# Run dbos tests using SQLite by default and record results in DuckDB.
test backend="sqlite":
    DBOS_TEST_BACKEND={{ backend }} DBOS_TEST_PREFLIGHT=true scripts/record-tests.sh go test -json -count=1 -timeout 30m ./dbos

# Query the latest recorded test run.
results:
    duckdb -readonly .test-results/latest.duckdb -f scripts/test-results.sql
