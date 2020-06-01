import logging
import os
import threading
from contextlib import contextmanager
from time import sleep
from typing import Callable

import pytest
import requests

from myrabbit import EventBus, EventBusAdapter, Service
from myrabbit.commands.command_bus import CommandBus
from myrabbit.commands.command_bus_adapter import CommandBusAdapter
from myrabbit.core.consumer.consumer import Consumer
from myrabbit.core.publisher.reconnecting_publisher import (
    PublisherFactory,
    ReconnectingPublisherFactory,
)

from .setup_logging import setup_logging

logger = logging.getLogger(__name__)
setup_logging()


api_host = "localhost:15672"
amqp_host = "localhost:5672"
vhost = "myrabbit_test"


def pytest_sessionstart(session) -> None:
    requests.delete(f"http://{api_host}/api/vhosts/{vhost}", auth=("guest", "guest"))
    requests.put(f"http://{api_host}/api/vhosts/{vhost}", auth=("guest", "guest"))
    print(f"Re-created vhost {vhost!r}")


def pytest_sessionfinish(session) -> None:
    if os.getenv("MYRABBIT_DONT_CLEAR_VHOST", False):
        return
    requests.delete(f"http://{api_host}/api/vhosts/{vhost}", auth=("guest", "guest"))
    print(f"Deleted vhost {vhost!r}")


@pytest.fixture
def rmq_url() -> str:
    return f"amqp://guest:guest@{amqp_host}/{vhost}"


@pytest.fixture
def run_consumer(rmq_url: str) -> Callable:
    @contextmanager
    def runner(consumer: Consumer):
        def worker() -> None:
            try:
                consumer.run()
            except Exception:
                logger.exception("Exception happened in consumer %s", consumer)

        thread = threading.Thread(target=worker)

        try:
            thread.start()

            sleep(1)

            yield
        finally:
            consumer.stop()
            thread.join()

    return runner


@pytest.fixture()
def publisher_factory(rmq_url: str) -> PublisherFactory:
    return ReconnectingPublisherFactory(rmq_url)


@pytest.fixture()
def event_bus(publisher_factory) -> EventBus:
    return EventBus(publisher_factory)


@pytest.fixture()
def event_bus_adapter(event_bus: EventBus) -> EventBusAdapter:
    return EventBusAdapter(event_bus)


@pytest.fixture()
def command_bus(publisher_factory: PublisherFactory) -> CommandBus:
    return CommandBus(publisher_factory)


@pytest.fixture()
def command_bus_adapter(command_bus: CommandBus) -> CommandBusAdapter:
    return CommandBusAdapter(command_bus)


@pytest.fixture()
def make_service(publisher_factory):
    def factory(name: str) -> Service:
        return Service(name, EventBus(publisher_factory), CommandBus(publisher_factory))

    return factory
