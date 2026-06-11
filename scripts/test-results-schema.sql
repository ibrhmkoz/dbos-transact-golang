CREATE TABLE IF NOT EXISTS test_runs (
    run_id VARCHAR PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP NOT NULL,
    duration_seconds DOUBLE NOT NULL,
    status VARCHAR NOT NULL,
    exit_code INTEGER NOT NULL,
    backend VARCHAR NOT NULL,
    race BOOLEAN NOT NULL,
    pattern VARCHAR,
    command VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS test_events (
    run_id VARCHAR NOT NULL,
    event_index BIGINT,
    event_time TIMESTAMP,
    action VARCHAR NOT NULL,
    package VARCHAR,
    test VARCHAR,
    elapsed_seconds DOUBLE,
    output VARCHAR
);

CREATE TABLE IF NOT EXISTS test_results (
    run_id VARCHAR NOT NULL,
    package VARCHAR NOT NULL,
    test VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    elapsed_seconds DOUBLE,
    parallel BOOLEAN NOT NULL DEFAULT false,
    output VARCHAR,
    PRIMARY KEY (run_id, package, test)
);

CREATE OR REPLACE VIEW latest_test_run AS
SELECT *
FROM test_runs
ORDER BY started_at DESC
LIMIT 1;

-- Leaf tests only: a parent test groups subtests and is excluded.
CREATE OR REPLACE VIEW leaf_test_results AS
SELECT results.*
FROM test_results AS results
WHERE NOT EXISTS (
    SELECT 1
    FROM test_results AS child
    WHERE child.run_id = results.run_id
      AND child.package = results.package
      AND starts_with(child.test, results.test || '/')
);

CREATE OR REPLACE VIEW latest_test_results AS
SELECT results.*
FROM leaf_test_results AS results
JOIN latest_test_run AS latest USING (run_id);

CREATE OR REPLACE VIEW failed_test_results AS
SELECT runs.started_at, runs.backend, runs.race, results.*
FROM leaf_test_results AS results
JOIN test_runs AS runs USING (run_id)
WHERE results.status = 'fail';
