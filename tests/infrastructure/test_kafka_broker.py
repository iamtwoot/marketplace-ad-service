from typing import Any

from src.application.tracing import TRACE_ID_HEADER, reset_trace_id, set_trace_id
from src.infrastructure.messaging.kafka_broker import KafkaMessageBroker


class FakeProducer:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def send_and_wait(
        self,
        topic: str,
        value: dict[str, Any],
        headers: list[tuple[str, bytes]],
    ) -> None:
        self.calls.append({"topic": topic, "value": value, "headers": headers})


async def test_send_puts_trace_id_into_headers() -> None:
    producer = FakeProducer()
    broker = KafkaMessageBroker(producer, "ads")

    token = set_trace_id("trace-kafka")
    try:
        await broker.send({"event": "ad.created", "payload": {"ad_id": 1}})
    finally:
        reset_trace_id(token)

    assert producer.calls == [
        {
            "topic": "ads",
            "value": {"event": "ad.created", "payload": {"ad_id": 1}},
            "headers": [(TRACE_ID_HEADER, b"trace-kafka")],
        }
    ]


async def test_send_without_trace_id_has_no_headers() -> None:
    producer = FakeProducer()
    broker = KafkaMessageBroker(producer, "ads")

    await broker.send({"event": "ad.created", "payload": {"ad_id": 1}})

    assert producer.calls[0]["headers"] == []
