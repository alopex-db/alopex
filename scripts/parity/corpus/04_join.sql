-- 04_join.sql: INNER/LEFT/RIGHT/FULL/CROSS JOIN, USING (実テーブル同士のみ)
-- 前提: 01_ddl.sql + 02_dml.sql 実行済み。読み取り専用。
-- orders.user_id = 9 は users に存在しない孤児行 (RIGHT/FULL JOIN 検証用)。
-- users.region = 'north' は regions に存在しない (LEFT JOIN USING 検証用)。
-- 注: 派生テーブル (FROM 句サブクエリ) の JOIN は CLI 実測で ALOPEX-C003 となるため
--     本コーパスから除外している (既知ギャップ、解消後に追補)。

-- INNER JOIN (ON 等値条件)
SELECT users.name, orders.id AS order_id, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id ORDER BY orders.id;

-- 裸の JOIN (INNER と等価) + WHERE
SELECT users.name, orders.id AS order_id FROM users JOIN orders ON users.id = orders.user_id WHERE orders.amount > 50.0 ORDER BY orders.id;

-- LEFT JOIN (注文のない dave/frank は order_id が NULL)
SELECT users.id AS user_id, users.name, orders.id AS order_id FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id ASC, orders.id ASC NULLS LAST;

-- RIGHT JOIN (孤児注文 15 は name が NULL)
SELECT orders.id AS order_id, users.name FROM users RIGHT JOIN orders ON users.id = orders.user_id ORDER BY orders.id;

-- FULL JOIN (両側の非マッチ行を含む)
SELECT users.id AS user_id, orders.id AS order_id FROM users FULL JOIN orders ON users.id = orders.user_id ORDER BY users.id ASC NULLS LAST, orders.id ASC NULLS LAST;

-- CROSS JOIN (6 users x 3 regions = 18 行)
SELECT users.id AS user_id, regions.id AS region_id FROM users CROSS JOIN regions ORDER BY users.id, regions.id;

-- USING (共有カラム region による等値結合)
SELECT users.name, regions.bonus FROM users INNER JOIN regions USING (region) ORDER BY users.id;

-- LEFT JOIN USING (region='north' の erin は bonus が NULL)
SELECT users.name, regions.bonus FROM users LEFT JOIN regions USING (region) ORDER BY users.id;

-- テーブルエイリアス付き INNER JOIN (CLI 実測で動作確認済みの形)
SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id ORDER BY o.amount;

-- 3 テーブル連鎖 JOIN (erin は region='north' が regions に無いため脱落)
SELECT users.name, orders.id AS order_id, regions.bonus FROM users INNER JOIN orders ON users.id = orders.user_id INNER JOIN regions ON users.region = regions.region ORDER BY orders.id;
