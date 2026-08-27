CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);
INSERT INTO docs VALUES
  (1, 'the quick brown fox'),
  (2, 'quick database search'),
  (3, 'unrelated text');

SELECT TO_TSVECTOR('simple', body) AS vector FROM docs ORDER BY id;
SELECT row_id, document, rank, headline
FROM FTS_SEARCH('docs', 'body', 'quick')
ORDER BY row_id;

CREATE INDEX docs_body_fts ON docs(body) USING FTS;
SELECT row_id, document, rank, headline
FROM FTS_SEARCH('docs', 'body', 'quick')
ORDER BY row_id;

UPDATE docs SET body = 'slow database' WHERE id = 2;
DELETE FROM docs WHERE id = 1;
SELECT row_id FROM FTS_SEARCH('docs', 'body', 'quick') ORDER BY row_id;
