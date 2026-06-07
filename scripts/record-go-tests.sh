#!/usr/bin/env bash

set -euo pipefail

if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required to record test results" >&2
    exit 2
fi

if ! command -v duckdb >/dev/null 2>&1; then
    echo "duckdb is required to record test results" >&2
    exit 2
fi

db_path="${TEST_RESULTS_DB:-.test-results/tests.duckdb}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
backend="${DBOS_TEST_BACKEND:-postgres}"
race="${TEST_RACE:-false}"
pattern="${TEST_PATTERN:-}"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
started_epoch="$(date +%s)"
events_file="$(mktemp "${TMPDIR:-/tmp}/go-test-events.XXXXXX")"

cleanup() {
    rm -f "$events_file"
}
trap cleanup EXIT

mkdir -p "$(dirname "$db_path")"

command_display="$(printf '%q ' "$@")"
printf 'Recording test run %s in %s\n' "$run_id" "$db_path"

set +e
"$@" 2>&1 | tee "$events_file" | jq -jr 'select(.Action == "output" or .Action == "build-output") | .Output'
pipeline_status=("${PIPESTATUS[@]}")
set -e

test_exit_code="${pipeline_status[0]}"
if [[ "${pipeline_status[1]}" -ne 0 || "${pipeline_status[2]}" -ne 0 ]]; then
    echo "Failed to capture the Go test JSON stream" >&2
    exit 2
fi

finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
finished_epoch="$(date +%s)"
duration_seconds="$((finished_epoch - started_epoch))"
status="pass"
if [[ "$test_exit_code" -ne 0 ]]; then
    status="fail"
fi

if ! TEST_RUN_ID="$run_id" \
    TEST_STARTED_AT="$started_at" \
    TEST_FINISHED_AT="$finished_at" \
    TEST_DURATION_SECONDS="$duration_seconds" \
    TEST_STATUS="$status" \
    TEST_EXIT_CODE="$test_exit_code" \
    DBOS_TEST_BACKEND="$backend" \
    TEST_RACE="$race" \
    TEST_PATTERN="$pattern" \
    TEST_COMMAND="$command_display" \
    TEST_EVENTS_FILE="$events_file" \
    duckdb "$db_path" \
        -f "$script_dir/test-results-schema.sql" \
        -f "$script_dir/load-test-results.sql" >/dev/null; then
    echo "Failed to record test run in $db_path" >&2
    echo "Close write-capable DuckDB connections, or open the database read-only while tests run." >&2
    exit 2
fi

printf '\nRecorded run %s: %s in %ss\n' "$run_id" "$status" "$duration_seconds"
exit "$test_exit_code"
