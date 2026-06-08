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
    PRIMARY KEY (run_id, package, test)
);

ALTER TABLE test_results
ADD COLUMN IF NOT EXISTS parallel BOOLEAN DEFAULT false;

CREATE TABLE IF NOT EXISTS package_results (
    run_id VARCHAR NOT NULL,
    package VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    elapsed_seconds DOUBLE,
    PRIMARY KEY (run_id, package)
);

CREATE OR REPLACE VIEW latest_test_run AS
SELECT *
FROM test_runs
ORDER BY started_at DESC
LIMIT 1;

CREATE OR REPLACE VIEW latest_test_results AS
SELECT results.*
FROM test_results AS results
JOIN latest_test_run AS latest USING (run_id);

CREATE OR REPLACE VIEW latest_leaf_test_results AS
SELECT results.*
FROM latest_test_results AS results
WHERE NOT EXISTS (
    SELECT 1
    FROM latest_test_results AS child
    WHERE child.run_id = results.run_id
      AND child.package = results.package
      AND starts_with(child.test, results.test || '/')
);

CREATE OR REPLACE VIEW latest_test_parallelism AS
SELECT
    parallel,
    count(*) AS tests,
    round(sum(elapsed_seconds), 3) AS elapsed_seconds
FROM latest_leaf_test_results
GROUP BY parallel
ORDER BY parallel;

CREATE OR REPLACE VIEW latest_package_results AS
SELECT results.*
FROM package_results AS results
JOIN latest_test_run AS latest USING (run_id);

CREATE OR REPLACE VIEW latest_test_events AS
SELECT events.*
FROM test_events AS events
JOIN latest_test_run AS latest USING (run_id);

CREATE OR REPLACE VIEW failed_test_results AS
SELECT runs.started_at, runs.backend, runs.race, results.*
FROM test_results AS results
JOIN test_runs AS runs USING (run_id)
WHERE results.status = 'fail';

CREATE OR REPLACE VIEW failed_package_results AS
SELECT runs.started_at, runs.backend, runs.race, results.*
FROM package_results AS results
JOIN test_runs AS runs USING (run_id)
WHERE results.status = 'fail';

CREATE OR REPLACE VIEW test_duration_history AS
SELECT
    results.test,
    results.package,
    count(*) AS runs,
    avg(results.elapsed_seconds) AS avg_seconds,
    max(results.elapsed_seconds) AS max_seconds,
    quantile_cont(results.elapsed_seconds, 0.95) AS p95_seconds
FROM test_results AS results
WHERE results.status = 'pass'
GROUP BY results.test, results.package;
