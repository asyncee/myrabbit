import logging
import os
import threading
from contextlib import contextmanager
from time import sleep

import pytest
import requests

from myrabbit import EventBus
from myrabbit import EventBusAdapter
from myrabbit.core.consumer.consumer import Consumer
from .logging import setup_logging

logger = logging.getLogger(__name__)
setup_logging()


api_host = "localhost:15672"
amqp_host = "localhost:5672"
vhost = "myrabbit_test"


def pytest_sessionstart(session):
    requests.delete(f"http://{api_host}/api/vhosts/{vhost}", auth=("guest", "guest"))
    requests.put(f"http://{api_host}/api/vhosts/{vhost}", auth=("guest", "guest"))
    print(f"Re-created vhost {vhost!r}")


def pytest_sessionfinish(session):
    if os.getenv("MYRABBIT_DONT_CLEAR_VHOST", False):
        return
    requests.delete(f"http://{api_host}/api/vhosts/{vhost}", auth=("guest", "guest"))
    print(f"Deleted vhost {vhost!r}")


@pytest.fixture
def rmq_url() -> str:
    return f"amqp://guest:guest@{amqp_host}/{vhost}"


@pytest.fixture
def run_consumer(rmq_url):
    @contextmanager
    def runner(consumer: Consumer):
        def worker():
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
def event_bus(rmq_url):
    return EventBus(
        rmq_url,
        default_exchange_params={"auto_delete": True},
        default_queue_params={"auto_delete": True},
    )


@pytest.fixture()
def event_bus_adapter(event_bus):
    return EventBusAdapter(event_bus)
