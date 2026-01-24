#!/usr/bin/env bash
set -euo pipefail

ROOT="$(pwd)"
DATA_DIR="$(mktemp -d)"
trap 'rm -rf "$DATA_DIR"' EXIT

cd "$ROOT"

run_sql() {
  local title="$1"
  local sql="$2"
  echo
  echo "== $title =="
  cargo run -p alopex-cli -- --data-dir "$DATA_DIR" sql "$sql"
}

echo "Preparing SQL demo data..."

run_sql "1) DDL: CREATE TABLE (IF NOT EXISTS) + DROP TABLE (IF EXISTS)" "
DROP TABLE IF EXISTS products;
CREATE TABLE IF NOT EXISTS products (
  id INT PRIMARY KEY,
  category TEXT NOT NULL,
  region TEXT,
  price DOUBLE,
  tag TEXT
);
"

run_sql "2) DML: INSERT (multi-row)" "
INSERT INTO products (id, category, region, price, tag) VALUES
  (1, 'book', 'us', 10.0, 'new'),
  (2, 'book', 'us', 15.0, 'sale'),
  (3, 'book', 'eu', 5.0, NULL),
  (4, 'game', 'us', 20.0, 'hit'),
  (5, 'game', 'eu', 7.0, 'new'),
  (6, 'toy', 'jp', 3.0, 'kids');
"

run_sql "3) SELECT + WHERE + ORDER BY + LIMIT/OFFSET" "
SELECT id, category, price
FROM products
WHERE price >= 7
ORDER BY price DESC, id ASC
LIMIT 3 OFFSET 0;
"

run_sql "4) DML: UPDATE + SELECT" "
UPDATE products SET price = price + 1.0 WHERE category = 'book';
SELECT id, price FROM products WHERE category = 'book' ORDER BY id;
"

run_sql "5) DML: DELETE + COUNT(*)" "
DELETE FROM products WHERE category = 'toy';
SELECT COUNT(*) FROM products;
"

run_sql "6) GROUP BY + aggregates" "
SELECT
  category,
  COUNT(*),
  COUNT(price),
  COUNT(DISTINCT tag),
  SUM(price),
  TOTAL(price),
  AVG(price),
  MIN(price),
  MAX(price)
FROM products
GROUP BY category
ORDER BY category;
"

run_sql "7) HAVING (filters grouped results)" "
SELECT category, COUNT(*)
FROM products
GROUP BY category
HAVING COUNT(*) > 1
ORDER BY category;
"

run_sql "8) GROUP_CONCAT (custom separator)" "
SELECT category, GROUP_CONCAT(tag, '|')
FROM products
GROUP BY category
ORDER BY category;
"

run_sql "9) STRING_AGG (custom separator)" "
SELECT category, STRING_AGG(tag, ';')
FROM products
GROUP BY category
ORDER BY category;
"

run_sql "10) DISTINCT projection" "
SELECT DISTINCT category
FROM products
ORDER BY category;
"

run_sql "11) Vector functions (literal table)" "
SELECT
  vector_similarity([1.0, 0.0], [0.0, 1.0], 'cosine') AS cos_sim,
  vector_distance([1.0, 0.0], [2.0, 0.0], 'l2') AS l2_dist,
  vector_dims([1.0, 2.0, 3.0]) AS dims,
  vector_norm([3.0, 4.0]) AS norm;
"

run_sql "12) VECTOR column + HNSW index + KNN ORDER BY" "
DROP TABLE IF EXISTS items;
CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));
CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW WITH (m = 8, ef_construction = 32);
INSERT INTO items (id, embedding) VALUES
  (1, [0.0, 0.0]),
  (2, [1.0, 0.0]),
  (3, [0.5, 0.0]);
SELECT id
FROM items
ORDER BY vector_similarity(embedding, [0.8, 0.0], 'l2') ASC
LIMIT 2;
"

echo
echo "SQL demo completed."
