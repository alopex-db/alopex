-- 07_vector.sql: VECTOR 列, vector_similarity/distance, HNSW KNN (ORDER BY + LIMIT)
-- 前提: 01_ddl.sql + 02_dml.sql 実行済み (docs 5 行 + HNSW インデックス)。
-- 本ファイル末尾で docs を 1 行更新 + 1 行削除する (99_verify.sql はその後の状態を検証)。
-- すべてのベクトル成分・距離は f32 で正確に表現できる値 (ピタゴラス数ベース) で設計。

-- ベクトル関数 (リテラル同士, sql-demo.sh シナリオ 11 準拠)
SELECT vector_similarity([1.0, 0.0], [0.0, 1.0], 'cosine') AS cos_orthogonal, vector_similarity([1.0, 2.0, 2.0], [2.0, 4.0, 4.0], 'cosine') AS cos_parallel, vector_similarity([1.0, 2.0, 2.0], [-1.0, -2.0, -2.0], 'cosine') AS cos_opposite;

SELECT vector_distance([0.0, 0.0], [3.0, 4.0], 'l2') AS l2_dist, vector_similarity([1.0, 2.0, 3.0], [4.0, 5.0, 6.0], 'inner') AS inner_product, vector_dims([1.0, 2.0, 3.0]) AS dims, vector_norm([3.0, 4.0]) AS norm;

-- カラムとクエリベクトルの距離 (クエリ点 [1,0,0] からの L2 距離)
SELECT docs.id, docs.title, vector_distance(docs.embedding, [1.0, 0.0, 0.0], 'l2') AS dist FROM docs ORDER BY docs.id;

-- HNSW KNN Top-3 (L2 + ASC + LIMIT: インデックス経路)
SELECT docs.id FROM docs ORDER BY vector_distance(docs.embedding, [1.0, 0.0, 0.0], 'l2') ASC LIMIT 3;

-- KNN + 距離の射影
SELECT docs.id, vector_distance(docs.embedding, [1.0, 0.0, 0.0], 'l2') AS dist FROM docs ORDER BY vector_distance(docs.embedding, [1.0, 0.0, 0.0], 'l2') ASC LIMIT 2;

-- WHERE 句での距離フィルタ (0.0 / 1.25 / 2.5 の 3 行が一致)
SELECT docs.id FROM docs WHERE vector_distance(docs.embedding, [1.0, 0.0, 0.0], 'l2') <= 2.5 ORDER BY docs.id;

-- vector_norm (カラム, ノルムが正確な行のみ対象)
SELECT docs.id, vector_norm(docs.embedding) AS norm FROM docs WHERE docs.id = 1;

-- ベクトル UPDATE (HNSW インデックス更新: echo を [1,0,0] の近傍 dist=0.25 へ移動)
UPDATE docs SET embedding = [1.0, 0.25, 0.0] WHERE id = 5;

-- ベクトル行 DELETE (HNSW インデックスからの削除)
DELETE FROM docs WHERE id = 4;

-- 更新/削除反映後の KNN Top-3 (dist: id1=0.0, id5=0.25, id2=1.25)
SELECT docs.id FROM docs ORDER BY vector_distance(docs.embedding, [1.0, 0.0, 0.0], 'l2') ASC LIMIT 3;

SELECT COUNT(*) AS doc_count FROM docs;

-- cosine 類似度による並べ替え (第 2 ソートキー付きのため KNN 最適化外 = フルスキャン経路。
-- cos([1,0,0]): id1=1.0, id5≈0.970, id3≈0.371, id2≈0.243 で降順マージンが大きく順位は f32 誤差に対して安定)
SELECT docs.id FROM docs ORDER BY vector_similarity(docs.embedding, [1.0, 0.0, 0.0], 'cosine') DESC, docs.id ASC LIMIT 3;
