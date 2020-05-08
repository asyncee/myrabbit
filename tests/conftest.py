import logging
import os
import threading
from contextlib import contextmanager
from time import sleep

import pytest

from myrabbit.core.consumer.basic_consumer import BasicConsumer
from .logging import setup_logging

logger = logging.getLogger(__name__)
setup_logging()


@pytest.fixture
def rmq_url() -> str:
    return os.getenv("MYRABBIT_TEST_RMQ_URL", "amqp://guest:guest@localhost:5672/%2F")


@pytest.fixture
def run_consumer(rmq_url):
    @contextmanager
    def runner(consumer: BasicConsumer):
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
