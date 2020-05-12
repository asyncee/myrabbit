import logging
import threading
import time
from dataclasses import dataclass
from time import sleep
from typing import Callable

import pytest

from myrabbit import EventBus
from myrabbit.core.consumer.basic_consumer import BasicConsumer
from myrabbit.service import Service


@dataclass
class EmptyEvent:
    pass


@pytest.mark.benchmark
def test_events_throughput(
    event_bus: EventBus, run_consumer: Callable, rmq_url: str
) -> None:
    """
    You should run this test with

    `pytest -s tests/test_throughput.py -m benchmark`
    """

    logging.getLogger("myrabbit").setLevel(logging.ERROR)

    received = 0
    sent = 0

    s = Service("Counter", event_bus)

    @s.on(
        "Counter",
        EmptyEvent,
        exchange_params={"auto_delete": True, "durable": False},
        queue_params={"auto_delete": True, "durable": False},
    )
    def increment_received(event) -> None:
        nonlocal received
        received += 1

    stop = False

    def counter(start_time: int):
        print()
        while not stop:
            elapsed = time.monotonic() - start_time
            mps = int(received / elapsed)
            print(
                f"Elapsed: {elapsed:.2f}, messages per second: {mps}, "
                f"sent: {sent}, received: {received}"
            )
            sleep(1)

    def publish_message() -> None:
        nonlocal sent
        while not stop:
            s.publish(EmptyEvent())
            sent += 1

    consumer = BasicConsumer(rmq_url, s.listeners)
    with run_consumer(consumer):
        # Thread pool should be used there.
        t1 = threading.Thread(target=counter, args=(time.monotonic(),))
        t2 = threading.Thread(target=publish_message)

        t1.start()
        t2.start()

        sleep(10)
        stop = True

        t1.join()
        t2.join()
