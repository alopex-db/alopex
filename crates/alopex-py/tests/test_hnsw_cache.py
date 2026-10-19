"""Public cache controls, transaction isolation and bounded LRU retention (#672)."""
import asyncio

import pytest
from alopex import Database, EmbeddedConfig, TxnMode

QUERY = "SELECT id FROM items ORDER BY vector_distance(e, [0,0], 'l2') LIMIT 1 WITH (enable_hnsw = true)"


def seed(db):
    db.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, e VECTOR(2,L2))")
    db.execute_sql("INSERT INTO items VALUES (1,[1,0]), (2,[5,0])")
    db.execute_sql("CREATE INDEX ih ON items (e) USING HNSW")


@pytest.mark.parametrize('mode', [TxnMode.READ_ONLY, TxnMode.READ_WRITE])
@pytest.mark.parametrize('warm', [False, True])
def test_transaction_cache_reused_across_statements(mode, warm):
    db = Database.new()
    seed(db)
    if warm:
        assert db.warm_hnsw_index('ih') > 0
    before = db.hnsw_cache_stats()
    tx = db.begin(mode)
    for _ in range(5):
        assert tx.execute_sql(QUERY) == [{'id': 1}]
    after = db.hnsw_cache_stats()
    assert after['misses'] - before['misses'] == (0 if warm else 1)
    assert after['hits'] - before['hits'] >= 4
    # A private load is never published to shared state at statement boundaries.
    assert after['entries'] == (1 if warm else 0)
    tx.rollback()
    db.close()


@pytest.mark.parametrize('commit', [False, True])
def test_copy_on_write_and_old_snapshot(commit):
    db = Database.new()
    seed(db)
    db.warm_hnsw_index('ih')
    old = db.begin(TxnMode.READ_ONLY)
    writer = db.begin(TxnMode.READ_WRITE)
    before = db.hnsw_cache_stats()
    writer.execute_sql('INSERT INTO items VALUES (3,[0,0])')
    assert writer.execute_sql(QUERY) == [{'id': 3}]
    assert old.execute_sql(QUERY) == [{'id': 1}]
    assert db.execute_sql(QUERY) == [{'id': 1}]
    assert db.hnsw_cache_stats()['misses'] == before['misses'], 'copy-on-write must not reload KV'
    if commit:
        writer.commit()
    else:
        writer.rollback()
    assert old.execute_sql(QUERY) == [{'id': 1}]
    old.rollback()
    assert db.execute_sql(QUERY) == [{'id': 3 if commit else 1}]
    db.close()


def test_savepoint_restores_sql_graph_and_prepared_reuse():
    db = Database.new()
    seed(db)
    db.warm_hnsw_index('ih')
    tx = db.begin(TxnMode.READ_WRITE)
    assert tx.execute_sql(QUERY) == [{'id': 1}]
    tx.savepoint('a')
    tx.execute_sql('INSERT INTO items VALUES (3,[0,0])')
    assert tx.execute_sql(QUERY) == [{'id': 3}]
    tx.rollback_to('a')
    ps = tx.prepare(QUERY)
    assert ps.execute() == [{'id': 1}]
    before = db.hnsw_cache_stats()['misses']
    assert ps.execute() == [{'id': 1}]
    assert db.hnsw_cache_stats()['misses'] == before
    ps.finalize()
    tx.commit()
    db.close()


def test_limits_oversize_disabled_and_stats(tmp_path):
    db = Database.open(str(tmp_path / 'db'), hnsw_cache_bytes=10_000_000, hnsw_cache_entries=2)
    seed(db)
    size = db.warm_hnsw_index('ih')
    assert size > 0
    stats = db.hnsw_cache_stats()
    assert stats['entries'] == 1
    assert stats['used_bytes'] == sum(e['bytes'] for e in stats['indexes']) == size
    assert stats['limit_entries'] == 2
    db.set_hnsw_cache_limit(size - 1)
    before = db.hnsw_cache_stats()
    for _ in range(2):
        assert db.execute_sql(QUERY) == [{'id': 1}]
    after = db.hnsw_cache_stats()
    assert after['entries'] == after['used_bytes'] == 0
    assert after['oversize_skips'] >= before['oversize_skips'] + 2
    assert after['misses'] == before['misses'] + 2
    db.set_hnsw_cache_limit(0)
    assert db.warm_hnsw_index('ih') == 0
    with pytest.raises(ValueError, match='positive'):
        db.set_hnsw_cache_limit(100, entries=0)
    with pytest.raises(Exception):
        db.warm_hnsw_index('missing')
    db.close()
    with pytest.raises(Exception, match='closed'):
        db.hnsw_cache_stats()


def test_config_and_invalid_limits_do_not_create_files(tmp_path):
    config = EmbeddedConfig(hnsw_cache_bytes=1000, hnsw_cache_entries=3)
    db = Database.open_with_config(config)
    assert db.hnsw_cache_stats()['limit_bytes'] == 1000
    assert db.hnsw_cache_stats()['limit_entries'] == 3
    db.close()
    path = tmp_path / 'invalid'
    with pytest.raises(ValueError):
        Database.open(str(path), hnsw_cache_entries=0)
    assert not path.exists()


def test_lru_two_indexes_alternating_low_and_high_budget():
    db = Database.new(hnsw_cache_entries=2)
    for name in ['a', 'b']:
        db.execute_sql(f'CREATE TABLE {name} (id INTEGER PRIMARY KEY, e VECTOR(2,L2))')
        db.execute_sql(f'INSERT INTO {name} VALUES (1,[0,0]),(2,[5,0])')
        db.execute_sql(f'CREATE INDEX {name}_h ON {name} (e) USING HNSW')
    sizes = [db.get_hnsw_stats(name + '_h').memory_bytes for name in ['a', 'b']]
    assert min(sizes) > 0
    db.set_hnsw_cache_limit(max(sizes))
    before = db.hnsw_cache_stats()
    for name in ['a', 'b', 'a', 'b']:
        assert db.warm_hnsw_index(name + '_h') > 0
    low = db.hnsw_cache_stats()
    assert low['misses'] - before['misses'] >= 3
    assert low['evictions'] - before['evictions'] >= 3
    db.set_hnsw_cache_limit(sum(sizes))
    for name in ['a', 'b']:
        db.warm_hnsw_index(name + '_h')
    before = db.hnsw_cache_stats()
    for name in ['a', 'b', 'a', 'b']:
        db.warm_hnsw_index(name + '_h')
    after = db.hnsw_cache_stats()
    assert after['entries'] == 2
    assert after['misses'] == before['misses']
    assert after['evictions'] == before['evictions']
    db.close()


def test_small_exact_query_reads_metadata_without_loading_graph():
    db = Database.new()
    seed(db)
    before = db.hnsw_cache_stats()
    assert db.execute_sql(QUERY.replace(' WITH (enable_hnsw = true)', '')) == [{'id': 1}]
    after = db.hnsw_cache_stats()
    assert after['misses'] == before['misses']
    assert after['entries'] == 0
    db.close()


def test_async_cache_controls():
    from alopex.asyncio import AsyncDatabase
    async def run():
        db = await AsyncDatabase.new(hnsw_cache_bytes=1024, hnsw_cache_entries=3)
        assert (await db.hnsw_cache_stats())['limit_bytes'] == 1024
        await db.set_hnsw_cache_limit(2048, entries=1)
        assert (await db.hnsw_cache_stats())['limit_entries'] == 1
        await db.close()
    asyncio.run(run())


@pytest.mark.parametrize('mode', [TxnMode.READ_ONLY, TxnMode.READ_WRITE])
def test_runtime_disable_releases_transaction_retention(mode):
    db = Database.new()
    seed(db)
    db.warm_hnsw_index('ih')
    tx = db.begin(mode)
    assert tx.execute_sql(QUERY) == [{'id': 1}]
    db.set_hnsw_cache_limit(0)
    before = db.hnsw_cache_stats()['misses']
    for _ in range(2):
        assert tx.execute_sql(QUERY) == [{'id': 1}]
    assert db.hnsw_cache_stats()['misses'] == before + 2
    tx.rollback()
    db.close()


def test_drop_recreate_index_discards_transaction_graph():
    db = Database.new()
    seed(db)
    db.warm_hnsw_index('ih')
    tx = db.begin(TxnMode.READ_WRITE)
    assert tx.execute_sql(QUERY) == [{'id': 1}]
    tx.execute_sql('DROP INDEX ih')
    tx.execute_sql('UPDATE items SET e=[0,0] WHERE id=2')
    tx.execute_sql('CREATE INDEX ih ON items (e) USING HNSW')
    assert tx.execute_sql(QUERY) == [{'id': 2}]
    tx.commit()
    assert db.execute_sql(QUERY) == [{'id': 2}]
    db.close()


@pytest.mark.parametrize('mode', [TxnMode.READ_ONLY, TxnMode.READ_WRITE])
def test_disabling_cache_drops_empty_transaction_graphs(mode):
    db = Database.new()
    db.execute_sql('CREATE TABLE items (id INTEGER PRIMARY KEY, e VECTOR(2,L2))')
    db.execute_sql('CREATE INDEX ih ON items (e) USING HNSW')
    assert db.execute_sql(QUERY) == []
    assert db.hnsw_cache_stats()['entries'] == 1
    tx = db.begin(mode)
    db.set_hnsw_cache_limit(0)
    assert db.hnsw_cache_stats()['entries'] == 0
    before = db.hnsw_cache_stats()['misses']
    for _ in range(2):
        assert tx.execute_sql(QUERY) == []
    assert db.hnsw_cache_stats()['misses'] == before + 2
    tx.rollback()
    db.close()
