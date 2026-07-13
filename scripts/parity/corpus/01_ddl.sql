-- 01_ddl.sql: CREATE/DROP TABLE, CREATE/DROP INDEX (BTREE / HNSW)
-- 前提: 空のデータベースに対して実行する。
-- 各文は ';' 区切りで 1 文ずつ順次実行される。

-- 冪等化: 既存テーブルを掃除する (空 DB では no-op)
DROP TABLE IF EXISTS users;

DROP TABLE IF EXISTS orders;

DROP TABLE IF EXISTS products;

DROP TABLE IF EXISTS regions;

DROP TABLE IF EXISTS docs;

-- 基本テーブル (スカラ型 + 制約)
CREATE TABLE users (
  id INT PRIMARY KEY,
  name TEXT NOT NULL,
  region TEXT,
  age INT,
  score DOUBLE,
  active BOOLEAN
);

-- IF NOT EXISTS: 既存テーブルに対する no-op
CREATE TABLE IF NOT EXISTS users (
  id INT PRIMARY KEY,
  name TEXT NOT NULL,
  region TEXT,
  age INT,
  score DOUBLE,
  active BOOLEAN
);

CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT NOT NULL,
  amount DOUBLE NOT NULL,
  status TEXT NOT NULL
);

CREATE TABLE products (
  id INT PRIMARY KEY,
  category TEXT NOT NULL,
  region TEXT,
  price DOUBLE,
  tag TEXT
);

-- UNIQUE 制約付きテーブル (JOIN USING の相手側)
CREATE TABLE regions (
  id INT PRIMARY KEY,
  region TEXT UNIQUE,
  bonus DOUBLE
);

-- VECTOR 型テーブル (L2 メトリクス, 3 次元)
CREATE TABLE docs (
  id INT PRIMARY KEY,
  title TEXT,
  embedding VECTOR(3, L2)
);

-- BTREE インデックス (明示指定)
CREATE INDEX idx_orders_user_id ON orders (user_id) USING BTREE;

-- IF NOT EXISTS: 既存インデックスに対する no-op
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders (user_id) USING BTREE;

-- メソッド無指定 (デフォルト BTREE)
CREATE INDEX idx_products_category ON products (category);

-- HNSW インデックス (sql-demo.sh シナリオ 12 準拠)
CREATE INDEX idx_docs_embedding ON docs (embedding) USING HNSW WITH (m = 8, ef_construction = 32);

-- DROP TABLE の実動作確認用の一時テーブル
CREATE TABLE scratch (
  id INT PRIMARY KEY,
  note TEXT
);

DROP TABLE scratch;

-- IF EXISTS: 存在しないテーブルに対する no-op
DROP TABLE IF EXISTS scratch;

-- DROP INDEX の実動作確認
CREATE INDEX idx_users_region ON users (region);

DROP INDEX idx_users_region;

-- IF EXISTS: 存在しないインデックスに対する no-op
DROP INDEX IF EXISTS idx_users_region;
