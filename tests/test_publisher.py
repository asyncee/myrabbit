import logging
import random
from queue import Queue
from time import sleep
from typing import Callable

import pytest

from myrabbit.core.consumer.consumer import Consumer
from myrabbit.core.consumer.listener import Exchange
from myrabbit.core.consumer.listener import Listener
from myrabbit.core.consumer.listener import Queue as Q
from myrabbit.core.consumer.pika_message import PikaMessage
from myrabbit.core.consumer.reply import Reply
from myrabbit.core.publisher import make_publisher

logger = logging.getLogger(__name__)


def test_basic_publisher_multisend(rmq_url: str, run_consumer: Callable) -> None:
    queue: Queue = Queue()

    def callback(msg: PikaMessage) -> None:
        queue.put(msg)

    exchange = "exchange_" + str(random.randint(100000, 999999))
    queue_name = "queue_" + str(random.randint(100000, 999999))

    listeners = [
        Listener(
            exchange=Exchange(type="topic", name=exchange, auto_delete=True),
            queue=Q(queue_name, auto_delete=True),
            routing_key="test",
            handle_message=callback,
        )
    ]

    consumer = Consumer(rmq_url, listeners)

    to_send = [b"A", b"B", b"C"]
    with run_consumer(consumer), make_publisher(rmq_url) as publisher:
        for message in to_send:
            publisher.publish(exchange, "test", message)
        sleep(1)

    assert queue.qsize() == 3
    assert to_send == sorted([queue.get().body for _ in range(3)])


def test_publisher_rpc_with_direct_reply(rmq_url: str, run_consumer: Callable) -> None:
    exchange = "exchange_" + str(random.randint(100000, 999999))
    queue_name = "queue_" + str(random.randint(100000, 999999))

    def callback(msg: PikaMessage) -> Reply:
        return Reply(body=msg.body + b"-reply")

    listeners = [
        Listener(
            exchange=Exchange(type="topic", name=exchange, auto_delete=True),
            queue=Q(queue_name, auto_delete=True),
            routing_key="test",
            handle_message=callback,
        )
    ]

    consumer = Consumer(rmq_url, listeners)

    with run_consumer(consumer), make_publisher(rmq_url) as publisher:
        response = publisher.rpc(exchange, "test", b"aaa", timeout=None)
        assert isinstance(response, PikaMessage)
        assert response.body == b"aaa-reply"
        logger.info("Received first response: %s", response.body)

        response2 = publisher.rpc(exchange, "test", b"bbb", timeout=None)
        assert isinstance(response2, PikaMessage)
        assert response2.body == b"bbb-reply"
        logger.info("Received second response: %s", response.body)


def test_publisher_rpc_with_direct_reply_timeout(
    rmq_url: str, run_consumer: Callable
) -> None:
    exchange = "exchange_" + str(random.randint(100000, 999999))
    queue_name = "queue_" + str(random.randint(100000, 999999))

    def callback(msg: PikaMessage) -> Reply:
        sleep(2)
        return Reply(body=msg.body + b"-reply")

    listeners = [
        Listener(
            exchange=Exchange(type="topic", name=exchange, auto_delete=True,),
            queue=Q(queue_name, auto_delete=True),
            routing_key="test",
            handle_message=callback,
        )
    ]

    consumer = Consumer(rmq_url, listeners)

    with run_consumer(consumer), make_publisher(rmq_url) as publisher:
        with pytest.raises(TimeoutError):
            publisher.rpc(exchange, "test", b"yyy", timeout=1)


def test_publisher_rpc_with_direct_reply_timeout_but_message_replied_faster(
    rmq_url: str, run_consumer: Callable
) -> None:
    exchange = "exchange_" + str(random.randint(100000, 999999))
    queue_name = "queue_" + str(random.randint(100000, 999999))

    def callback(msg: PikaMessage) -> Reply:
        return Reply(body=msg.body + b"-reply")

    listeners = [
        Listener(
            exchange=Exchange(type="topic", name=exchange, auto_delete=True,),
            queue=Q(queue_name, auto_delete=True),
            routing_key="test",
            handle_message=callback,
        )
    ]

    consumer = Consumer(rmq_url, listeners)

    with run_consumer(consumer), make_publisher(rmq_url) as publisher:
        response = publisher.rpc(exchange, "test", b"xxx", timeout=10)
        assert response.body == b"xxx-reply"
