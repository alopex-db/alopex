import asyncio

import pytest

from alopex import AlopexError
from alopex.asyncio import AsyncChangefeed, AsyncDatabase


class _FakeChangefeed:
    def __init__(self) -> None:
        self.status = {"operation_state": "accepted"}
        self.events = [{"key": "first"}]
        self.calls: list[tuple[object, ...]] = []
        self.context_exited = False

    def subscribe(self, generation: int, epoch: int, request_id: str) -> dict[str, object]:
        self.calls.append(("subscribe", generation, epoch, request_id))
        self.status = {"operation_state": "running"}
        return {"outcome": "subscribed"}

    def poll(self, max_events: int, request_id: str) -> dict[str, object]:
        self.calls.append(("poll", max_events, request_id))
        return {"events": [], "outcome": "polled"}

    def stream(self, max_events: int, request_id: str) -> dict[str, object]:
        self.calls.append(("stream", max_events, request_id))
        return {"events": [], "outcome": "streamed"}

    def ack(self, ack_id: str, checkpoint: str, request_id: str) -> dict[str, object]:
        self.calls.append(("ack", ack_id, checkpoint, request_id))
        return {"result": {"ack_state": "accepted"}}

    def resume(self, checkpoint: str, request_id: str) -> dict[str, object]:
        self.calls.append(("resume", checkpoint, request_id))
        return {"outcome": "resumed"}

    def cancel(self, request_id: str) -> dict[str, object]:
        self.calls.append(("cancel", request_id))
        self.status = {"operation_state": "cancelled"}
        return {"outcome": "cancelled"}

    def close(self, request_id: str) -> dict[str, object]:
        self.calls.append(("close", request_id))
        self.status = {"operation_state": "closed"}
        return {"outcome": "closed"}

    def __next__(self) -> dict[str, object]:
        if not self.events:
            raise StopIteration
        return self.events.pop(0)

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        self.context_exited = True
        self.status = {"operation_state": "closed"}
        return False


def test_async_changefeed_delegates_full_lifecycle_and_async_protocols():
    async def scenario() -> None:
        native = _FakeChangefeed()
        feed = AsyncChangefeed(native, single_thread=True)

        assert await feed.subscribe(3, 9, "subscribe-request") == {"outcome": "subscribed"}
        assert await feed.poll(10, "poll-request") == {
            "events": [],
            "outcome": "polled",
        }
        assert await feed.stream(10, "stream-request") == {
            "events": [],
            "outcome": "streamed",
        }
        assert await feed.ack("ack-a", "checkpoint-a", "ack-request") == {
            "result": {"ack_state": "accepted"}
        }
        assert await feed.resume("checkpoint-a", "resume-request") == {"outcome": "resumed"}
        assert await feed.__anext__() == {"key": "first"}
        with pytest.raises(StopAsyncIteration):
            await feed.__anext__()
        assert await feed.cancel("cancel-request") == {"outcome": "cancelled"}
        assert await feed.close("close-request") == {"outcome": "closed"}
        assert feed.status == {"operation_state": "closed"}

        async with feed as entered:
            assert entered is feed
        assert native.context_exited

    asyncio.run(scenario())


def test_async_changefeed_task_cancellation_requests_native_terminal_cleanup():
    async def scenario() -> None:
        native = _FakeChangefeed()
        feed = AsyncChangefeed(native, single_thread=False)
        pending = asyncio.create_task(feed.__anext__())
        await asyncio.sleep(0)
        pending.cancel()

        with pytest.raises(asyncio.CancelledError):
            await pending

        assert native.calls[-1][0] == "cancel"
        assert native.calls[-1][1].startswith("async-iterator-cancel-")
        assert native.status == {"operation_state": "cancelled"}

    asyncio.run(scenario())


def test_async_changefeed_preserves_native_timeout_status():
    class _TimeoutChangefeed(_FakeChangefeed):
        def stream(self, max_events: int, request_id: str) -> dict[str, object]:
            super().stream(max_events, request_id)
            failure = AlopexError("changefeed deadline elapsed")
            failure.code = "changefeed_timeout"
            failure.status = {
                "failure_class": "timeout",
                "reason_code": "deadline_elapsed",
            }
            raise failure

    async def scenario() -> None:
        feed = AsyncChangefeed(_TimeoutChangefeed(), single_thread=False)
        with pytest.raises(AlopexError) as timeout:
            await feed.stream(10, "timeout-request")
        assert timeout.value.code == "changefeed_timeout"
        assert timeout.value.status == {
            "failure_class": "timeout",
            "reason_code": "deadline_elapsed",
        }

    asyncio.run(scenario())


def test_async_database_preserves_native_changefeed_prerequisite_failure():
    async def scenario() -> None:
        database = await AsyncDatabase.open_in_memory()
        try:
            with pytest.raises(AlopexError) as failure:
                await database.create_changefeed(
                    "async-feed",
                    cluster_id="cluster-a",
                    table_id=7,
                    range_id="range-a",
                    generation=3,
                    schema_version=4,
                    data_epoch=9,
                    request_id="create-request",
                )
            assert failure.value.code == "changefeed_prerequisite_missing"
            assert failure.value.status["reason_code"] == "durable_capability_missing"
        finally:
            await database.close()

    asyncio.run(scenario())
