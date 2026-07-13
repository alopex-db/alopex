-- 05_aggregate.sql: GROUP BY, HAVING, COUNT/SUM/AVG/MIN/MAX/TOTAL/GROUP_CONCAT/STRING_AGG
-- 前提: 01_ddl.sql + 02_dml.sql 実行済み。読み取り専用。
-- orders は 7 行 (合計 294.0)、products は 5 行 (book 3 行 / game 2 行)。
-- 金額はすべて 0.25 の倍数で設計してあり、SUM/AVG は 2 進浮動小数点で正確に表現できる。

-- グローバル集約 (COUNT の NULL 除外: dave の score は NULL)
SELECT COUNT(*) AS user_count, COUNT(score) AS score_count, COUNT(DISTINCT region) AS region_count FROM users;

-- グローバル集約 (294.0 / 7 = 42.0)
SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount, AVG(amount) AS avg_amount, MIN(amount) AS min_amount, MAX(amount) AS max_amount, TOTAL(amount) AS grand_total FROM orders;

-- GROUP BY + 複数集約
SELECT category, COUNT(*) AS cnt, SUM(price) AS total_price, AVG(price) AS avg_price, MIN(price) AS min_price, MAX(price) AS max_price FROM products GROUP BY category ORDER BY category;

-- HAVING (グループのフィルタ)
SELECT region, COUNT(*) AS cnt FROM users GROUP BY region HAVING COUNT(*) > 1 ORDER BY region;

-- HAVING に射影外の集約 (book: 33.0 > 30.0, game: 27.5)
SELECT category FROM products GROUP BY category HAVING SUM(price) > 30.0 ORDER BY category;

-- GROUP_CONCAT (カスタム区切り, NULL はスキップ, グループ内はスキャン順 = 主キー順)
SELECT category, GROUP_CONCAT(tag, '|') AS tags FROM products GROUP BY category ORDER BY category;

-- STRING_AGG (カスタム区切り。注: 文区切りの ';' と衝突するため区切り文字に ';' は使わない)
SELECT category, STRING_AGG(tag, '#') AS tags FROM products GROUP BY category ORDER BY category;

-- 複数キー GROUP BY
SELECT category, region, COUNT(*) AS cnt, SUM(price) AS total_price FROM products GROUP BY category, region ORDER BY category, region;

-- 空入力のグローバル集約 (COUNT=0, SUM/AVG/MIN は NULL, TOTAL は 0.0)
SELECT COUNT(*) AS cnt, SUM(price) AS total_price, TOTAL(price) AS grand_total, AVG(price) AS avg_price, MIN(price) AS min_price FROM products WHERE price > 1000.0;

-- 空入力の GROUP BY (グループ無し = 0 行)
SELECT category, COUNT(*) AS cnt FROM products WHERE price > 1000.0 GROUP BY category ORDER BY category;

-- JOIN + GROUP BY (east: 125.5 + 90.0, north: 12.75, west: 20.25)
SELECT users.region, SUM(orders.amount) AS region_total FROM users INNER JOIN orders ON users.id = orders.user_id GROUP BY users.region ORDER BY users.region;

-- NULL を含むカラムの COUNT (book の tag は 'new','sale',NULL)
SELECT category, COUNT(tag) AS tag_count FROM products GROUP BY category ORDER BY category;
