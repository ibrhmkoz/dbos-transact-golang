BEGIN TRANSACTION;

CREATE TEMP TABLE incoming_test_events AS
SELECT
    getenv('TEST_RUN_ID') AS run_id,
    row_number() OVER () AS event_index,
    try_cast(json_extract_string(json, '$.Time') AS TIMESTAMP) AS event_time,
    json_extract_string(json, '$.Action') AS action,
    coalesce(
        json_extract_string(json, '$.Package'),
        json_extract_string(json, '$.ImportPath')
    ) AS package,
    json_extract_string(json, '$.Test') AS test,
    try_cast(json_extract_string(json, '$.Elapsed') AS DOUBLE) AS elapsed_seconds,
    json_extract_string(json, '$.Output') AS output
FROM read_json_objects(getenv('TEST_EVENTS_FILE'), format = 'newline_delimited')
WHERE json_extract_string(json, '$.Action') IS NOT NULL;

INSERT INTO test_runs (
    run_id,
    started_at,
    finished_at,
    duration_seconds,
    status,
    exit_code,
    backend,
    race,
    pattern,
    command
)
SELECT
    getenv('TEST_RUN_ID'),
    getenv('TEST_STARTED_AT')::TIMESTAMP,
    getenv('TEST_FINISHED_AT')::TIMESTAMP,
    getenv('TEST_DURATION_SECONDS')::DOUBLE,
    getenv('TEST_STATUS'),
    getenv('TEST_EXIT_CODE')::INTEGER,
    getenv('DBOS_TEST_BACKEND'),
    getenv('TEST_RACE')::BOOLEAN,
    nullif(getenv('TEST_PATTERN'), ''),
    getenv('TEST_COMMAND');

INSERT INTO test_events (
    run_id,
    event_index,
    event_time,
    action,
    package,
    test,
    elapsed_seconds,
    output
)
SELECT * FROM incoming_test_events;

INSERT INTO test_results (run_id, package, test, status, elapsed_seconds, parallel, output)
SELECT
    results.run_id,
    results.package,
    results.test,
    results.action,
    results.elapsed_seconds,
    EXISTS (
        SELECT 1
        FROM incoming_test_events AS paused
        WHERE paused.run_id = results.run_id
          AND paused.package = results.package
          AND paused.action = 'pause'
          AND (
              paused.test = results.test
              OR starts_with(results.test, paused.test || '/')
          )
    ),
    (
        SELECT string_agg(events.output, '' ORDER BY events.event_index)
        FROM incoming_test_events AS events
        WHERE events.package = results.package
          AND events.test = results.test
          AND events.output IS NOT NULL
    )
FROM incoming_test_events AS results
WHERE results.test IS NOT NULL
  AND results.action IN ('pass', 'fail', 'skip');

COMMIT;
