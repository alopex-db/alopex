-- 99_verify.sql: 各シナリオの幕末で実行する共通アサート
-- 前提: 01〜07 実行後の最終状態。成功が確認できる構文のみ使用 (JOIN 可、サブクエリ不可)。
--   users: 6 行 / orders: 7 行 (合計 294.0) / products: 5 行 / regions: 3 行 / docs: 4 行

SELECT COUNT(*) AS user_count FROM users;

SELECT COUNT(*) AS order_count, SUM(amount) AS order_total, AVG(amount) AS order_avg FROM orders;

SELECT COUNT(*) AS product_count, MIN(price) AS min_price, MAX(price) AS max_price FROM products;

SELECT COUNT(*) AS region_count FROM regions;

SELECT COUNT(*) AS doc_count FROM docs;

-- JOIN を跨ぐ集約 (孤児注文 15 の 45.5 を除いた 6 件 = 248.5)
SELECT COUNT(*) AS joined_orders, SUM(orders.amount) AS joined_total FROM users INNER JOIN orders ON users.id = orders.user_id;

SELECT category, COUNT(*) AS cnt, SUM(price) AS total_price FROM products GROUP BY category ORDER BY category;

-- KNN Top-3 (07_vector.sql での更新/削除反映後: dist id1=0.0, id5=0.25, id2=1.25)
SELECT docs.id FROM docs ORDER BY vector_distance(docs.embedding, [1.0, 0.0, 0.0], 'l2') ASC LIMIT 3;
