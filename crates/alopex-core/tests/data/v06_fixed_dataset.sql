-- Deterministic 100k-row dataset fixture for v0.5/v0.6 benchmark comparison.
-- The benchmark harness should execute this before measurement.

CREATE TABLE IF NOT EXISTS v06_fixed_dataset (
    id BIGINT PRIMARY KEY,
    bucket INT NOT NULL,
    payload TEXT NOT NULL,
    score DOUBLE
);

DELETE FROM v06_fixed_dataset;

WITH RECURSIVE seq(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < 100000
)
INSERT INTO v06_fixed_dataset (id, bucket, payload, score)
SELECT
    n,
    n % 1024,
    'payload-' || n,
    (n % 1000) / 1000.0
FROM seq;
