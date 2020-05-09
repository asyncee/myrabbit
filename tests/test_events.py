from __future__ import annotations

import queue
from dataclasses import dataclass
from functools import partial
from time import sleep

from pydantic import BaseModel

from myrabbit import EventBus
from myrabbit import EventBusAdapter
from myrabbit.core.consumer.basic_consumer import BasicConsumer
from myrabbit.events.event_with_message import EventWithMessage
from myrabbit.events.listen_event_strategy import Broadcast
from myrabbit.events.listen_event_strategy import ServicePool
from myrabbit.events.listen_event_strategy import Singleton


class BasicClassEvent:
    def __init__(self, name: str):
        self.name = name


class PydanticEvent(BaseModel):
    name: str


@dataclass
class DataclassEvent:
    name: str


def test_events(
    rmq_url, run_consumer, event_bus: EventBus, event_bus_adapter: EventBusAdapter
):
    q = queue.Queue()

    pydantic_event = PydanticEvent(name="pydantic-event")
    dataclass_event = DataclassEvent(name="dataclass-event")

    def callback(msg: EventWithMessage):
        q.put(msg)

    listeners = [
        event_bus.listener("service-a", "service-b", "OrderConfirmed", callback),
        event_bus_adapter.listener("service-a", "service-b", DataclassEvent, callback),
        event_bus_adapter.listener("service-a", "service-b", PydanticEvent, callback),
    ]

    consumer = BasicConsumer(rmq_url, listeners)

    with run_consumer(consumer):
        event_bus.publish("service-b", "OrderConfirmed", {"order_id": 10})
        event_bus_adapter.publish("service-b", dataclass_event)
        event_bus_adapter.publish("service-b", pydantic_event)

    message = q.get(block=True, timeout=1)
    assert isinstance(message, EventWithMessage)
    assert message.event == {"order_id": 10}

    message2 = q.get(block=True, timeout=1)
    assert isinstance(message, EventWithMessage)
    assert message2.event == dataclass_event

    message3 = q.get(block=True, timeout=1)
    assert isinstance(message, EventWithMessage)
    assert message3.event == pydantic_event


def test_events_singleton(
    rmq_url, run_consumer, event_bus: EventBus,
):
    q = queue.Queue()

    def callback(msg: EventWithMessage):
        q.put(msg)

    listeners = [
        event_bus.listener(
            "service-a", "service-b", "event", callback, listen_strategy=Singleton()
        ),
        event_bus.listener(
            "service-c", "service-b", "event", callback, listen_strategy=Singleton()
        ),
    ]

    consumer = BasicConsumer(rmq_url, listeners)

    with run_consumer(consumer):
        event_bus.publish("service-b", "event", {"name": "test-event"})
        sleep(1)

    assert q.qsize() == 1
    message = q.get(block=True, timeout=1)
    assert isinstance(message, EventWithMessage)
    assert message.event == {"name": "test-event"}


def test_events_service_pool(
    rmq_url, run_consumer, event_bus: EventBus,
):
    q = queue.Queue()

    def callback(msg: EventWithMessage, catched_from: str):
        q.put((msg, catched_from))

    listeners = [
        event_bus.listener(
            "service-a",
            "service-b",
            "event",
            partial(callback, catched_from="service-a"),
            listen_strategy=ServicePool(),
        ),
        event_bus.listener(
            "service-c",
            "service-b",
            "event",
            partial(callback, catched_from="service-b"),
            listen_strategy=ServicePool(),
        ),
    ]

    consumer_a_1 = BasicConsumer(rmq_url, listeners)
    consumer_a_2 = BasicConsumer(rmq_url, listeners)
    consumer_b = BasicConsumer(rmq_url, listeners)

    with run_consumer(consumer_a_1):
        with run_consumer(consumer_a_2):
            with run_consumer(consumer_b):
                event_bus.publish("service-b", "event", {"name": "test-event"})
                sleep(1)

    assert q.qsize() == 2

    message, service = q.get(block=True, timeout=1)
    assert service in ("service-a", "service-b")
    assert isinstance(message, EventWithMessage)
    assert message.event == {"name": "test-event"}

    message2, service2 = q.get(block=True, timeout=1)
    assert service2 != service and service2 in ("service-a", "service-b")
    assert isinstance(message2, EventWithMessage)
    assert message2.event == {"name": "test-event"}


def test_events_broadcast(
    rmq_url, run_consumer, event_bus: EventBus,
):
    q = queue.Queue()

    def callback(msg: EventWithMessage, catched_from: str):
        q.put((msg, catched_from))

    listeners = [
        event_bus.listener(
            "service-a",
            "service-b",
            "event",
            partial(callback, catched_from="service-a-1"),
            listen_strategy=Broadcast(broadcast_identifier="service-a-instance"),
        ),
        event_bus.listener(
            "service-a",
            "service-b",
            "event",
            partial(callback, catched_from="service-a-1"),
            listen_strategy=Broadcast(broadcast_identifier="service-a-instance"),
        ),
        event_bus.listener(
            "service-a",
            "service-b",
            "event",
            partial(callback, catched_from="service-a-2"),
            listen_strategy=Broadcast(broadcast_identifier="service-a-instance-2"),
        ),
        event_bus.listener(
            "service-c",
            "service-b",
            "event",
            partial(callback, catched_from="service-b"),
            listen_strategy=Broadcast(),
        ),
    ]

    consumer_a_1 = BasicConsumer(rmq_url, listeners)
    consumer_a_2 = BasicConsumer(rmq_url, listeners)
    consumer_b = BasicConsumer(rmq_url, listeners)

    with run_consumer(consumer_a_1):
        with run_consumer(consumer_a_2):
            with run_consumer(consumer_b):
                event_bus.publish("service-b", "event", {"name": "test-event"})
                sleep(1)

    assert q.qsize() == 3

    expected_services = ("service-a-1", "service-a-2", "service-b")

    message, service = q.get(block=True, timeout=1)
    assert service in expected_services
    assert isinstance(message, EventWithMessage)
    assert message.event == {"name": "test-event"}

    message2, service2 = q.get(block=True, timeout=1)
    assert service2 != service and service2 in expected_services
    assert isinstance(message2, EventWithMessage)
    assert message2.event == {"name": "test-event"}

    message3, service3 = q.get(block=True, timeout=1)
    assert service3 not in (service, service2) and service3 in expected_services
    assert isinstance(message3, EventWithMessage)
    assert message3.event == {"name": "test-event"}
