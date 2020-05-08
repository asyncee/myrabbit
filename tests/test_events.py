from __future__ import annotations

import queue
from dataclasses import dataclass

from pydantic import BaseModel

from myrabbit.core.consumer.basic_consumer import BasicConsumer
from myrabbit.events.event_with_message import EventWithMessage


class BasicClassEvent:
    def __init__(self, name: str):
        self.name = name


class PydanticEvent(BaseModel):
    name: str


@dataclass
class DataclassEvent:
    name: str


def test_on_event(rmq_url, run_consumer, event_bus, event_bus_adapter):
    q = queue.Queue()

    pydantic_event = PydanticEvent(name="pydantic-event")
    dataclass_event = DataclassEvent(name="dataclass-event")

    def callback(msg: EventWithMessage):
        q.put(msg)

    listeners = [
        event_bus.listener("order-service", "OrderConfirmed", callback),
        event_bus_adapter.listener("order-service", DataclassEvent, callback),
        event_bus_adapter.listener("order-service", PydanticEvent, callback),
    ]

    consumer = BasicConsumer(rmq_url, listeners)

    with run_consumer(consumer):
        event_bus.publish("order-service", "OrderConfirmed", {"order_id": 10})
        event_bus_adapter.publish("order-service", dataclass_event)
        event_bus_adapter.publish("order-service", pydantic_event)

    message = q.get(block=True, timeout=1)
    assert isinstance(message, EventWithMessage)
    assert message.event == {"order_id": 10}

    message2 = q.get(block=True, timeout=1)
    assert isinstance(message, EventWithMessage)
    assert message2.event == dataclass_event

    message3 = q.get(block=True, timeout=1)
    assert isinstance(message, EventWithMessage)
    assert message3.event == pydantic_event
