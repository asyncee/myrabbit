import logging
import threading
import time
from dataclasses import dataclass
from time import sleep
from typing import Callable

import pytest

from myrabbit import EventBus
from myrabbit import EventWithMessage
from myrabbit.core.consumer.consumer import Consumer
from myrabbit.core.consumer.consumer import ThreadedConsumer
from myrabbit.service import Service


@dataclass
class EmptyEvent:
    pass


@pytest.mark.benchmark
@pytest.mark.parametrize(
    "consumer_class", [Consumer, ThreadedConsumer],
)
def test_events_throughput(
    consumer_class: Consumer,
    make_service: Callable,
    run_consumer: Callable,
    rmq_url: str,
) -> None:
    """
    You should run this test with

    `pytest -s tests/test_throughput.py -m benchmark`
    """
    print("Running with", consumer_class)

    logging.getLogger("myrabbit").setLevel(logging.ERROR)

    received = 0
    sent = 0

    s: Service = make_service("Counter")

    @s.on_event(
        "Counter",
        EmptyEvent,
        exchange_params={"auto_delete": True, "durable": False},
        queue_params={"auto_delete": True, "durable": False},
    )
    def increment_received(event: EventWithMessage) -> None:
        nonlocal received
        received += 1

    stop = False

    def counter(start_time: int) -> None:
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
        srv = Service("Counter", EventBus(rmq_url))
        nonlocal sent
        while not stop:
            srv.publish(EmptyEvent())
            sent += 1

    consumer = consumer_class(rmq_url, s.listeners)
    with run_consumer(consumer):
        # Thread pool should be used there.
        t1 = threading.Thread(target=counter, args=(time.monotonic(),))
        t2 = threading.Thread(target=publish_message)
        t3 = threading.Thread(target=publish_message)

        t1.start()
        t2.start()
        t3.start()

        sleep(10)
        stop = True

        t1.join()
        t2.join()
        t3.join()
