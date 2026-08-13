from alopex import Database, Transaction


def test_vector_api_is_part_of_every_build():
    database_methods = (
        "create_hnsw_index",
        "search_hnsw",
        "drop_hnsw_index",
        "get_hnsw_stats",
    )
    transaction_methods = (
        "upsert_vector",
        "search_similar",
        "get_vector",
        "upsert_to_hnsw",
        "delete_from_hnsw",
    )

    missing = [name for name in database_methods if not hasattr(Database, name)]
    missing.extend(name for name in transaction_methods if not hasattr(Transaction, name))

    assert missing == []
