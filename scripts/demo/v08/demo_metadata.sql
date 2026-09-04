CREATE TABLE "Order Items" (
    id BIGINT PRIMARY KEY,
    label TEXT DEFAULT 'new'
);
CREATE INDEX "Order Label" ON "Order Items" (label);
SHOW TABLES;
SHOW INDEXES FROM "Order Items";
DESCRIBE "Order Items";
SELECT table_name, table_type FROM information_schema.tables;
SELECT table_name, column_name, ordinal_position, column_default
FROM information_schema.columns
ORDER BY table_name, ordinal_position;
SELECT table_name, index_name, column_name
FROM information_schema.indexes
ORDER BY table_name, index_name, ordinal_position;
