CREATE TABLE inventory (id BIGINT, label TEXT);
INSERT INTO inventory VALUES (1, 'one'), (2, 'two');
CREATE VIEW inventory_labels AS SELECT label FROM inventory;
ALTER TABLE inventory ADD COLUMN quantity BIGINT DEFAULT 3;
ALTER TABLE inventory RENAME COLUMN label TO name;
SELECT name, quantity FROM inventory ORDER BY id;
DROP VIEW inventory_labels;
TRUNCATE TABLE inventory;
SELECT id FROM inventory;
