-- Recent test runs.
SELECT *
FROM test_runs
ORDER BY started_at DESC
LIMIT 20;

-- Failed tests from the latest run.
SELECT r.test, r.package, r.elapsed_seconds
FROM test_results r
WHERE r.run_id = (SELECT run_id FROM test_runs ORDER BY started_at DESC LIMIT 1)
  AND r.status = 'fail'
ORDER BY r.elapsed_seconds DESC;

-- Slowest tests across all recorded runs.
SELECT
    test,
    count(*) AS runs,
    round(avg(elapsed_seconds), 3) AS avg_seconds,
    round(max(elapsed_seconds), 3) AS max_seconds
FROM test_results
WHERE status = 'pass'
GROUP BY test
ORDER BY avg_seconds DESC
LIMIT 30;

-- Failure output from the latest run.
SELECT e.test, e.output
FROM test_events e
JOIN test_results r USING (run_id, package, test)
WHERE e.run_id = (SELECT run_id FROM test_runs ORDER BY started_at DESC LIMIT 1)
  AND r.status = 'fail'
  AND e.output IS NOT NULL
ORDER BY e.event_time;
