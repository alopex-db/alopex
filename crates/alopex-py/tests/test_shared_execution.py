from alopex import Database, SharedExecutionStep


def test_shared_execution_preserves_order_commit_metadata_and_query_result():
    database = Database.open_in_memory()
    database.execute_sql("CREATE TABLE shared_rows (id BIGINT PRIMARY KEY, value TEXT)")

    report = database.execute_shared(
        "execution-1",
        "transaction-1",
        [
            SharedExecutionStep.transaction_statement(
                "insert", "INSERT INTO shared_rows VALUES (1, 'committed')"
            ),
            SharedExecutionStep.commit_barrier("commit"),
            SharedExecutionStep.post_commit_read(
                "read", "SELECT id, value FROM shared_rows ORDER BY id"
            ),
        ],
    )

    assert report.execution_id == "execution-1"
    assert report.transaction_id == "transaction-1"
    assert [step.step_id for step in report.steps] == ["insert", "commit", "read"]
    assert [step.step_index for step in report.steps] == [0, 1, 2]
    assert [step.outcome_kind for step in report.steps] == [
        "execution",
        "commit",
        "execution",
    ]
    assert report.steps[0].result == 1
    assert report.steps[1].commit_metadata.transaction_id == "transaction-1"
    assert report.steps[2].result == [{"id": 1, "value": "committed"}]
    assert all(step.error is None for step in report.steps)


def test_shared_execution_returns_partial_outcome_after_committed_read_failure():
    database = Database.open_in_memory()
    database.execute_sql("CREATE TABLE durable_rows (id BIGINT PRIMARY KEY)")

    report = database.execute_shared(
        "execution-partial",
        "transaction-partial",
        [
            SharedExecutionStep.transaction_statement(
                "insert", "INSERT INTO durable_rows VALUES (7)"
            ),
            SharedExecutionStep.commit_barrier("commit"),
            SharedExecutionStep.post_commit_read("read", "SELECT * FROM missing_table"),
        ],
    )

    assert [step.outcome_kind for step in report.steps] == [
        "execution",
        "commit",
        "error",
    ]
    assert report.steps[1].commit_metadata.transaction_id == "transaction-partial"
    assert report.steps[2].error.kind == "post_commit_read"
    assert report.steps[2].error.message
    assert database.execute_sql("SELECT id FROM durable_rows") == [{"id": 7}]


def test_shared_execution_rejects_invalid_order_without_executing_later_steps():
    database = Database.open_in_memory()
    report = database.execute_shared(
        "execution-invalid",
        "transaction-invalid",
        [
            SharedExecutionStep.post_commit_read("early-read", "SELECT 1"),
            SharedExecutionStep.commit_barrier("commit"),
        ],
    )

    assert len(report.steps) == 1
    assert report.steps[0].step_id == "early-read"
    assert report.steps[0].outcome_kind == "error"
    assert report.steps[0].error.kind == "invalid_order"
