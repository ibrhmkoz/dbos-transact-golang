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

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
backend="${DBOS_TEST_BACKEND:-postgres}"
race="${TEST_RACE:-false}"
pattern="${TEST_PATTERN:-}"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
results_root="${TEST_RESULTS_DIR:-.test-results}"
workspace_db="${TEST_RESULTS_DB:-$results_root/results.duckdb}"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
started_epoch="$(date +%s)"
events_file="$(mktemp "${TMPDIR:-/tmp}/go-test-events.XXXXXX")"

mkdir -p "$results_root/runs"
if [[ "$results_root" = /* ]]; then
    results_root_absolute="$results_root"
else
    results_root_absolute="$PWD/$results_root"
fi

# Stage parquet files on the same filesystem as the final location so the
# move into place is an atomic rename. Readers never see a partial run.
staging_dir="$results_root/.staging-$run_id"
mkdir -p "$staging_dir"

cleanup() {
    rm -f "$events_file"
    rm -rf "$staging_dir"
}
trap cleanup EXIT

command_display="$(printf '%q ' "$@")"
printf 'Recording test run %s in %s/runs/%s\n' "$run_id" "$results_root" "$run_id"

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

if ! (
    cd "$staging_dir" &&
    TEST_RUN_ID="$run_id" \
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
    duckdb -f "$script_dir/load-test-results.sql" >/dev/null
); then
    echo "Failed to record test run $run_id" >&2
    exit 2
fi

mv "$staging_dir" "$results_root/runs/$run_id"

# Create the workspace database once: views over the per-run parquet files.
# It is never written to afterwards, so an open DataGrip connection to it
# cannot conflict with recording new runs.
if [[ ! -e "$workspace_db" ]]; then
    sed "s|__TEST_RESULTS_ROOT__|$results_root_absolute|g" \
        "$script_dir/test-results-schema.sql" | duckdb "$workspace_db" >/dev/null
fi

printf '\nRecorded run %s: %s in %ss\n' "$run_id" "$status" "$duration_seconds"
printf 'Query results: duckdb -readonly %s -f scripts/test-results.sql\n' "$workspace_db"
exit "$test_exit_code"
