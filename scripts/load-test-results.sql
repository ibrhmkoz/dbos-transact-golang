-- Transforms one `go test -json` event stream into immutable parquet files.
-- Runs in an in-memory DuckDB with cwd set to the run's staging directory;
-- the parquet files are written there and atomically moved into place by
-- record-tests.sh. Inputs come from environment variables (see record-tests.sh).

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

COPY (
    SELECT
        getenv('TEST_RUN_ID') AS run_id,
        getenv('TEST_STARTED_AT')::TIMESTAMP AS started_at,
        getenv('TEST_FINISHED_AT')::TIMESTAMP AS finished_at,
        getenv('TEST_DURATION_SECONDS')::DOUBLE AS duration_seconds,
        getenv('TEST_STATUS') AS status,
        getenv('TEST_EXIT_CODE')::INTEGER AS exit_code,
        getenv('DBOS_TEST_BACKEND') AS backend,
        getenv('TEST_RACE')::BOOLEAN AS race,
        nullif(getenv('TEST_PATTERN'), '') AS pattern,
        getenv('TEST_COMMAND') AS command
) TO 'runs.parquet' (FORMAT parquet);

COPY (
    SELECT * FROM incoming_test_events
) TO 'events.parquet' (FORMAT parquet);

COPY (
    SELECT
        results.run_id,
        results.package,
        results.test,
        results.action AS status,
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
        ) AS parallel,
        (
            SELECT string_agg(events.output, '' ORDER BY events.event_index)
            FROM incoming_test_events AS events
            WHERE events.package = results.package
              AND events.test = results.test
              AND events.output IS NOT NULL
        ) AS output
    FROM incoming_test_events AS results
    WHERE results.test IS NOT NULL
      AND results.action IN ('pass', 'fail', 'skip')
) TO 'results.parquet' (FORMAT parquet);
