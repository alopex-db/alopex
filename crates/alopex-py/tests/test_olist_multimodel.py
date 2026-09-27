import io

from alopex import Database, TxnMode


def _assert_olist_state(db):
    assert db.execute_sql(
        """
        SELECT c.customer_id, SUM(i.price) AS total
        FROM customers AS c
        JOIN orders AS o ON o.customer_id = c.customer_id
        JOIN order_items AS i ON i.order_id = o.order_id
        GROUP BY c.customer_id
        ORDER BY c.customer_id
        """
    ) == [{"customer_id": "customer-1", "total": 42}]
    assert db.execute_sql(
        """
        SELECT product_id
        FROM products
        ORDER BY vector_distance(embedding, ?, 'l2') ASC
        LIMIT 1
        """,
        [[0.0, 1.0]],
    ) == [{"product_id": 1}]
    assert db.execute_sql(
        """
        SELECT row_id
        FROM FTS_SEARCH('products', 'name', 'coffee') AS f(row_id, document, rank, headline)
        ORDER BY row_id
        """
    ) == [{"row_id": 1}]
    with db.begin(TxnMode.READ_ONLY) as txn:
        assert txn.get(b"order:101") == b"customer:customer-1"
        assert txn.get(b"order:102") is None


def test_olist_multimodel_commit_rollback_and_reopen(tmp_path):
    path = tmp_path / "olist.alopex"
    db = Database.open(str(path))
    db.execute_sql("CREATE TABLE customers (customer_id TEXT PRIMARY KEY, city TEXT)")
    db.execute_sql("CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id TEXT)")
    db.execute_sql(
        """
        CREATE TABLE order_items (order_id INTEGER, product_id INTEGER, price INTEGER)
        WITH (storage='columnar', compression='lz4')
        """
    )
    db.execute_sql(
        "CREATE TABLE products (product_id INTEGER PRIMARY KEY, name TEXT, embedding VECTOR(2, L2))"
    )
    db.execute_sql("CREATE INDEX products_embedding_hnsw ON products (embedding) USING HNSW")
    db.execute_sql("CREATE INDEX products_name_fts ON products (name) USING FTS")
    db.execute_sql("INSERT INTO customers VALUES ('customer-1', 'Sao Paulo')")
    db.execute_sql("INSERT INTO orders VALUES (101, 'customer-1')")
    assert db.copy_from_csv(
        "order_items",
        io.BytesIO(b"order_id,product_id,price\n101,1,30\n101,2,12\n"),
        header=True,
    ) == 2

    committed = db.begin(TxnMode.READ_WRITE)
    committed.execute_sql(
        "INSERT INTO products VALUES (?, ?, ?)", [1, "coffee beans", [1.0, 0.0]]
    )
    committed.put(b"order:101", b"customer:customer-1")
    committed.commit()
    del committed

    rolled_back = db.begin(TxnMode.READ_WRITE)
    rolled_back.execute_sql(
        "INSERT INTO products VALUES (?, ?, ?)", [2, "coffee mug", [0.0, 1.0]]
    )
    rolled_back.put(b"order:102", b"customer:customer-2")
    rolled_back.rollback()
    del rolled_back

    assert db.execute_sql("SELECT product_id FROM products ORDER BY product_id") == [
        {"product_id": 1}
    ]
    _assert_olist_state(db)

    db.close()
    reopened = Database.open(str(path))
    try:
        _assert_olist_state(reopened)
    finally:
        reopened.close()
