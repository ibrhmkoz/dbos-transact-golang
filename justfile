set dotenv-load := true

default:
    @just --list

# Run dbos tests using SQLite by default and record results in DuckDB.
test backend="sqlite":
    DBOS_TEST_BACKEND={{ backend }} scripts/record-go-tests.sh go test -json -count=1 -timeout 30m ./dbos
