import re
from typing import Any

import pytest

from src.application.ports.message_broker import MessageBroker
from src.application.services.outbox_relay import OutboxRelay
from src.application.tracing import get_trace_id, reset_trace_id, set_trace_id
from tests.conftest import FakeMessageBroker, FakeUnitOfWork


class RaisingBroker(MessageBroker):
    async def send(self, payload: dict[str, Any]) -> None:
        raise RuntimeError("kafka unavailable")


@pytest.mark.asyncio
async def test_relay_sends_and_marks_published(
    fake_uow: FakeUnitOfWork, fake_broker: FakeMessageBroker
) -> None:
    await fake_uow.outbox.add("ad.created", {"ad_id": 1})
    await fake_uow.outbox.add("ad.updated", {"ad_id": 1})

    relay = OutboxRelay(uow_factory=lambda: fake_uow, broker=fake_broker)
    processed = await relay._process_batch()

    assert processed == 2
    assert fake_broker.sent == [
        {"event": "ad.created", "payload": {"ad_id": 1}},
        {"event": "ad.updated", "payload": {"ad_id": 1}},
    ]
    assert fake_uow.committed
    assert fake_uow.outbox.messages == []


@pytest.mark.asyncio
async def test_relay_noop_when_outbox_empty(
    fake_uow: FakeUnitOfWork, fake_broker: FakeMessageBroker
) -> None:
    relay = OutboxRelay(uow_factory=lambda: fake_uow, broker=fake_broker)
    processed = await relay._process_batch()

    assert processed == 0
    assert fake_broker.sent == []
    assert not fake_uow.committed


@pytest.mark.asyncio
async def test_relay_rolls_back_and_leaves_messages_on_broker_failure(
    fake_uow: FakeUnitOfWork,
) -> None:
    await fake_uow.outbox.add("ad.created", {"ad_id": 1})

    relay = OutboxRelay(uow_factory=lambda: fake_uow, broker=RaisingBroker())

    with pytest.raises(RuntimeError):
        await relay._process_batch()

    assert not fake_uow.committed
    assert fake_uow.rolled_back
    assert len(fake_uow.outbox.messages) == 1


async def add_with_trace(uow: FakeUnitOfWork, trace_id: str, ad_id: int) -> None:
    token = set_trace_id(trace_id)
    try:
        await uow.outbox.add("ad.created", {"ad_id": ad_id})
    finally:
        reset_trace_id(token)


@pytest.mark.asyncio
async def test_relay_sends_each_message_with_its_trace_id(
    fake_uow: FakeUnitOfWork, fake_broker: FakeMessageBroker
) -> None:
    await add_with_trace(fake_uow, "trace-1", 1)
    await add_with_trace(fake_uow, "trace-2", 2)

    relay = OutboxRelay(uow_factory=lambda: fake_uow, broker=fake_broker)
    await relay._process_batch()

    assert fake_broker.sent_trace_ids == ["trace-1", "trace-2"]
    assert get_trace_id() is None


@pytest.mark.asyncio
async def test_relay_generates_trace_id_for_message_without_one(
    fake_uow: FakeUnitOfWork, fake_broker: FakeMessageBroker
) -> None:
    await fake_uow.outbox.add("ad.created", {"ad_id": 1})

    relay = OutboxRelay(uow_factory=lambda: fake_uow, broker=fake_broker)
    await relay._process_batch()

    [trace_id] = fake_broker.sent_trace_ids
    assert trace_id is not None
    assert re.fullmatch(r"[0-9a-f]{32}", trace_id)
    assert get_trace_id() is None
