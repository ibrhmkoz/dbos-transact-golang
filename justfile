set dotenv-load := true

default:
    @just --list

# Run the dbos package tests using SQLite by default.
test backend="sqlite":
    DBOS_TEST_BACKEND={{backend}} go test -v -count=1 -timeout 30m ./dbos

# Run dbos tests matching a Go test name or regular expression.
test-one pattern backend="sqlite":
    DBOS_TEST_BACKEND={{backend}} go test -v -count=1 -timeout 30m -run '{{pattern}}' ./dbos

# Run the dbos package tests with the race detector.
test-race backend="sqlite":
    DBOS_TEST_BACKEND={{backend}} go test -race -v -count=1 -timeout 30m ./dbos

# Run the same dbos package checks used by CI.
test-ci backend="sqlite":
    DBOS_TEST_BACKEND={{backend}} go vet ./dbos
    DBOS_TEST_BACKEND={{backend}} go test -race -v -count=1 -timeout 30m ./dbos

# Run tests for a package path, forwarding additional go test arguments.
test-package package args="":
    go test -v -count=1 -timeout 30m {{package}} {{args}}

# Vet the main dbos package.
vet:
    go vet ./dbos
