-- 03_query.sql: SELECT, WHERE, ORDER BY, LIMIT/OFFSET, DISTINCT
-- 前提: 01_ddl.sql + 02_dml.sql 実行済み。読み取り専用 (状態を変更しない)。

SELECT * FROM users ORDER BY id;

-- 射影 + エイリアス + AND 条件 (NULL スコアの dave は score >= 70.0 が NULL となり除外)
SELECT id, name AS user_name, score FROM users WHERE score >= 70.0 AND active = TRUE ORDER BY score DESC;

-- OR + 括弧 + IS NOT NULL
SELECT id, name FROM users WHERE region = 'east' OR (age IS NOT NULL AND age < 26) ORDER BY id;

SELECT id, name FROM users WHERE score IS NULL ORDER BY id;

SELECT id, name FROM users WHERE age IS NOT NULL ORDER BY id;

-- 算術式の射影 (DOUBLE * DOUBLE)
SELECT id, score * 2.0 AS double_score FROM users WHERE score IS NOT NULL ORDER BY id;

-- 文字列連結 ||
SELECT name || '@' || region AS label FROM users WHERE id <= 2 ORDER BY id;

-- ORDER BY DESC + LIMIT/OFFSET (id 降順 6,5,4,3,2,1 の 2 番目から 2 件)
SELECT id, name FROM users ORDER BY id DESC LIMIT 2 OFFSET 1;

-- NULLS FIRST (frank の age は NULL)
SELECT id, age FROM users ORDER BY age ASC NULLS FIRST, id ASC;

-- NULLS LAST + DESC
SELECT id, age FROM users ORDER BY age DESC NULLS LAST, id ASC;

SELECT DISTINCT region FROM users ORDER BY region;

-- 複数カラム DISTINCT
SELECT DISTINCT category, region FROM products ORDER BY category, region;

-- <> 比較 (tag が NULL の行は 3 値論理で除外される)
SELECT id, tag FROM products WHERE tag <> 'new' ORDER BY id;

-- NOT + 括弧
SELECT id, name FROM users WHERE NOT (region = 'east') ORDER BY id;

-- 範囲条件 (比較演算子の組み合わせ)
SELECT id, amount FROM orders WHERE amount >= 30.0 AND amount <= 60.0 ORDER BY id;
