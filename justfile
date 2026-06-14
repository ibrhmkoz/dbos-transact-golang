set dotenv-load

default:
    @just --list

# Regenerate database query code.
sqlc:
    sqlc generate

# Run dbos tests against PostgreSQL and record results in DuckDB.
test:
    scripts/record-tests.sh go test -json -count=1 -timeout 5m ./dbos
