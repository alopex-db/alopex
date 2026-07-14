-- 02_dml.sql: INSERT (複数行) / UPDATE / DELETE
-- 前提: 01_ddl.sql 実行済み。
-- このファイル完了時点の確定状態 (03 以降の全クエリの前提):
--   users:    6 行 (dave: age=29, active=FALSE に更新済み)
--   orders:   7 行 (id=13 削除済み, id=16 amount=12.75 に更新済み, 合計 294.0)
--   products: 5 行 (book 3 行は price +1.0 済み, toy 2 行削除済み)
--   regions:  3 行
--   docs:     5 行 (07_vector.sql で 1 行更新 + 1 行削除される)

INSERT INTO users (id, name, region, age, score, active) VALUES
  (1, 'alice', 'east', 30, 82.5, TRUE),
  (2, 'bob', 'west', 25, 74.25, TRUE),
  (3, 'carol', 'east', 35, 90.0, FALSE),
  (4, 'dave', 'west', 28, NULL, TRUE),
  (5, 'erin', 'north', 22, 65.5, FALSE),
  (6, 'frank', 'east', NULL, 71.75, TRUE);

INSERT INTO orders (id, user_id, amount, status) VALUES
  (10, 1, 50.0, 'shipped'),
  (11, 1, 75.5, 'pending'),
  (12, 2, 20.25, 'shipped'),
  (13, 2, 99.75, 'cancelled'),
  (14, 3, 30.0, 'pending'),
  (15, 9, 45.5, 'shipped'),
  (16, 5, 12.5, 'shipped'),
  (17, 3, 60.0, 'shipped');

INSERT INTO products (id, category, region, price, tag) VALUES
  (1, 'book', 'us', 10.0, 'new'),
  (2, 'book', 'us', 15.0, 'sale'),
  (3, 'book', 'eu', 5.0, NULL),
  (4, 'game', 'us', 20.0, 'hit'),
  (5, 'game', 'eu', 7.5, 'new'),
  (6, 'toy', 'jp', 3.25, 'kids'),
  (7, 'toy', 'jp', 4.75, NULL);

INSERT INTO regions (id, region, bonus) VALUES
  (1, 'east', 5.0),
  (2, 'west', 2.5),
  (3, 'south', 1.25);

-- ベクトル挿入 (成分・距離が f32 で正確に表現できる値のみ使用)
INSERT INTO docs (id, title, embedding) VALUES
  (1, 'alpha', [1.0, 0.0, 0.0]),
  (2, 'bravo', [0.25, 1.0, 0.0]),
  (3, 'charlie', [1.0, 1.5, 2.0]),
  (4, 'delta', [2.0, 2.0, 2.0]),
  (5, 'echo', [4.0, 4.0, 0.0]);

SELECT COUNT(*) AS user_count FROM users;

SELECT COUNT(*) AS order_count FROM orders;

-- UPDATE: 式による一括更新 (book 3 行: 10.0->11.0, 15.0->16.0, 5.0->6.0)
UPDATE products SET price = price + 1.0 WHERE category = 'book';

SELECT id, price FROM products WHERE category = 'book' ORDER BY id;

-- UPDATE: 複数カラム同時更新
UPDATE users SET age = 29, active = FALSE WHERE id = 4;

SELECT id, name, age, active FROM users WHERE id = 4;

-- UPDATE: DOUBLE 演算 (12.5 -> 12.75)
UPDATE orders SET amount = amount + 0.25 WHERE id = 16;

-- DELETE: 条件一致 1 行 (id=13)
DELETE FROM orders WHERE status = 'cancelled';

-- DELETE: 条件一致 2 行 (id=6, 7)
DELETE FROM products WHERE category = 'toy';

SELECT COUNT(*) AS order_count FROM orders;

SELECT COUNT(*) AS product_count FROM products;
