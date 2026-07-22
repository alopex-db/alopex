-- v0.8 local compatibility cases for the internal alopex-tools verifier.
-- Distributed-read routing is deliberately not executed by this embedded
-- binary; it is covered by the cluster/server/CLI integration surfaces.

SELECT lower('Alopex') AS lower_value,
       upper('db') AS upper_value,
       abs(-7) AS absolute_value;

SELECT category,
       COUNT(*) AS item_count,
       GROUP_CONCAT(tag, '|') AS tags
FROM products
GROUP BY category
ORDER BY category;

PRAGMA cache_size = 16;

PRAGMA io_stats;

SELECT clear_cache() AS cleared;
