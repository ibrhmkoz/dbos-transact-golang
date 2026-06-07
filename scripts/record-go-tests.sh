#!/usr/bin/env bash

set -uo pipefail

if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required to record test results" >&2
    exit 2
fi

if ! command -v duckdb >/dev/null 2>&1; then
    echo "duckdb is required to record test results" >&2
    exit 2
fi

db_path="${TEST_RESULTS_DB:-.test-results/tests.duckdb}"
backend="${DBOS_TEST_BACKEND:-postgres}"
race="${TEST_RACE:-false}"
pattern="${TEST_PATTERN:-}"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
started_epoch="$(date +%s)"
events_file="$(mktemp "${TMPDIR:-/tmp}/go-test-events.XXXXXX.jsonl")"
metadata_file="$(mktemp "${TMPDIR:-/tmp}/go-test-metadata.XXXXXX.json")"

cleanup() {
    rm -f "$events_file" "$metadata_file"
}
trap cleanup EXIT

mkdir -p "$(dirname "$db_path")"

command_display="$(printf '%q ' "$@")"
printf 'Recording test run %s in %s\n' "$run_id" "$db_path"

"$@" 2>&1 | tee "$events_file" | jq -jr 'select(.Action == "output" or .Action == "build-output") | .Output'
test_exit_code="${PIPESTATUS[0]}"

finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
finished_epoch="$(date +%s)"
duration_seconds="$((finished_epoch - started_epoch))"
status="pass"
if [[ "$test_exit_code" -ne 0 ]]; then
    status="fail"
fi

jq -n \
    --arg run_id "$run_id" \
    --arg started_at "$started_at" \
    --arg finished_at "$finished_at" \
    --arg status "$status" \
    --arg backend "$backend" \
    --arg pattern "$pattern" \
    --arg command "$command_display" \
    --argjson duration_seconds "$duration_seconds" \
    --argjson exit_code "$test_exit_code" \
    --argjson race "$race" \
    '{
        run_id: $run_id,
        started_at: $started_at,
        finished_at: $finished_at,
        duration_seconds: $duration_seconds,
        status: $status,
        exit_code: $exit_code,
        backend: $backend,
        race: $race,
        pattern: $pattern,
        command: $command
    }' >"$metadata_file"

if ! duckdb "$db_path" \
    -c "SET VARIABLE run_id = '$run_id';
CREATE TABLE IF NOT EXISTS test_runs (
    run_id VARCHAR PRIMARY KEY,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    duration_seconds DOUBLE,
    status VARCHAR,
    exit_code INTEGER,
    backend VARCHAR,
    race BOOLEAN,
    pattern VARCHAR,
    command VARCHAR
);
CREATE TABLE IF NOT EXISTS test_events (
    run_id VARCHAR,
    event_time TIMESTAMP,
    action VARCHAR,
    package VARCHAR,
    test VARCHAR,
    elapsed_seconds DOUBLE,
    output VARCHAR
);
CREATE TABLE IF NOT EXISTS test_results (
    run_id VARCHAR,
    package VARCHAR,
    test VARCHAR,
    status VARCHAR,
    elapsed_seconds DOUBLE,
    PRIMARY KEY (run_id, package, test)
);
CREATE TABLE IF NOT EXISTS package_results (
    run_id VARCHAR,
    package VARCHAR,
    status VARCHAR,
    elapsed_seconds DOUBLE,
    PRIMARY KEY (run_id, package)
);
INSERT INTO test_runs
SELECT
    run_id,
    started_at::TIMESTAMP,
    finished_at::TIMESTAMP,
    duration_seconds,
    status,
    exit_code,
    backend,
    race,
    pattern,
    command
FROM read_json_auto('$metadata_file');
INSERT INTO test_events
SELECT
    getvariable('run_id'),
    try_cast(json_extract_string(json, '$.Time') AS TIMESTAMP),
    json_extract_string(json, '$.Action'),
    json_extract_string(json, '$.Package'),
    json_extract_string(json, '$.Test'),
    try_cast(json_extract_string(json, '$.Elapsed') AS DOUBLE),
    json_extract_string(json, '$.Output')
FROM read_json_objects('$events_file', format = 'newline_delimited')
WHERE json_extract_string(json, '$.Action') IS NOT NULL;
INSERT INTO test_results
SELECT
    getvariable('run_id'),
    json_extract_string(json, '$.Package'),
    json_extract_string(json, '$.Test'),
    json_extract_string(json, '$.Action'),
    try_cast(json_extract_string(json, '$.Elapsed') AS DOUBLE)
FROM read_json_objects('$events_file', format = 'newline_delimited')
WHERE json_extract_string(json, '$.Test') IS NOT NULL
  AND json_extract_string(json, '$.Action') IN ('pass', 'fail', 'skip');
INSERT INTO package_results
SELECT
    getvariable('run_id'),
    json_extract_string(json, '$.Package'),
    json_extract_string(json, '$.Action'),
    try_cast(json_extract_string(json, '$.Elapsed') AS DOUBLE)
FROM read_json_objects('$events_file', format = 'newline_delimited')
WHERE json_extract_string(json, '$.Test') IS NULL
  AND json_extract_string(json, '$.Package') IS NOT NULL
  AND json_extract_string(json, '$.Action') IN ('pass', 'fail', 'skip');" >/dev/null; then
    echo "Failed to record test run in $db_path" >&2
    echo "Close write-capable DuckDB connections, or open the database read-only while tests run." >&2
    exit 2
fi

printf '\nRecorded run %s: %s in %ss\n' "$run_id" "$status" "$duration_seconds"
exit "$test_exit_code"
