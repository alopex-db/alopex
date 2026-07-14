-- 06_subquery.sql: スカラー / IN / EXISTS / ANY / ALL サブクエリ
-- 前提: 01_ddl.sql + 02_dml.sql 実行済み。読み取り専用。
--
-- 期待値 (expected/06_subquery.json) は SQL 意味論からの手計算値である。
-- verify.py コンテナ実測 (2026-07-13) では embedded / HTTP は全 9 文が期待値と一致。
-- CLI (WHERE 句サブクエリ: ALOPEX-E999、スカラーサブクエリ: 黙って空結果) と
-- gRPC (WHERE 句のみ成功) は期待値から逸脱する既知の製品バグであり、
-- S2-a のペア間 diff で INV-2 違反として検出・報告される。
--
-- 注: 派生テーブル (FROM 句サブクエリ) は CLI 実測で ALOPEX-C003 となり
--     エラー分類が安定確認できないため本コーパスから除外している。

-- 相関スカラーサブクエリ (射影)
SELECT users.name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users ORDER BY users.id;

-- 非相関スカラーサブクエリ (WHERE, AVG = 42.0 との比較)
SELECT orders.id AS order_id, orders.amount FROM orders WHERE orders.amount > (SELECT AVG(orders.amount) FROM orders) ORDER BY orders.id;

-- IN サブクエリ
SELECT users.name FROM users WHERE users.id IN (SELECT orders.user_id FROM orders) ORDER BY users.id;

-- NOT IN サブクエリ (サブクエリ結果に NULL は含まれない設計)
SELECT users.name FROM users WHERE users.id NOT IN (SELECT orders.user_id FROM orders) ORDER BY users.id;

-- 相関 EXISTS (amount > 70.0 の注文を持つのは alice のみ)
SELECT users.name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id AND orders.amount > 70.0) ORDER BY users.id;

-- 相関 NOT EXISTS
SELECT users.name FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY users.id;

-- ANY 量化比較 (game の価格 {20.0, 7.5} のいずれかより大きい = 7.5 より大きい)
SELECT products.id, products.price FROM products WHERE products.price > ANY (SELECT products.price FROM products WHERE products.category = 'game') ORDER BY products.id;

-- ALL 量化比較 (book の価格 {11.0, 16.0, 6.0} すべて以上 = 16.0 以上)
SELECT products.id, products.price FROM products WHERE products.price >= ALL (SELECT products.price FROM products WHERE products.category = 'book') ORDER BY products.id;

-- スカラーサブクエリ同士の算術 (75.5 - 12.75 = 62.75, FROM 句なし)
SELECT (SELECT MAX(orders.amount) FROM orders) - (SELECT MIN(orders.amount) FROM orders) AS amount_range;
