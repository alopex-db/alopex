-- 08_server_insert.sql: S1 第 3 幕でサーバー (SF-HTTP) 経由で実行する追加 INSERT
-- 前提: 01〜07 実行後の最終状態 (orders は id 10,11,12,14,15,16,17 の 7 行、合計 294.0)。
-- id 18/19 は既存 id と衝突しない。user_id 4 (dave) / 6 (frank) はともに既存ユーザーで、
-- これまで注文を持たなかったため JOIN 集約 (99_verify.sql 6 文目) の変化が可視化される。
-- 金額は 0.25 の倍数、かつ挿入後の合計 351.0 が行数 9 で割り切れる (AVG = 39.0) よう設計。

INSERT INTO orders (id, user_id, amount, status) VALUES
  (18, 4, 30.5, 'shipped'),
  (19, 6, 26.5, 'pending');
