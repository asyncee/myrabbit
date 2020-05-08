import logging
import random
from queue import Queue
from time import sleep

import pytest

from myrabbit.core.consumer.basic_consumer import BasicConsumer
from myrabbit.core.consumer.listener import Exchange
from myrabbit.core.consumer.listener import Listener
from myrabbit.core.consumer.listener import Queue as Q
from myrabbit.core.consumer.pika_message import PikaMessage
from myrabbit.core.publisher.basic_publisher import BasicPublisher

logger = logging.getLogger(__name__)


def test_basic_publisher_multisend(rmq_url, run_consumer):
    queue = Queue()

    def callback(msg: PikaMessage):
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

    consumer = BasicConsumer(rmq_url, listeners)

    publisher = BasicPublisher(rmq_url, exchange, "test")

    to_send = [b"A", b"B", b"C"]
    with run_consumer(consumer):
        publisher.publish_multiple(to_send)
        sleep(1)

    assert queue.qsize() == 3
    assert to_send == sorted([queue.get().body for _ in range(3)])


def test_publisher_with_direct_reply(rmq_url, run_consumer):
    exchange = "exchange_" + str(random.randint(100000, 999999))
    queue_name = "queue_" + str(random.randint(100000, 999999))

    def callback(msg: PikaMessage):
        return msg.body + b"-reply"

    listeners = [
        Listener(
            exchange=Exchange(type="topic", name=exchange, auto_delete=True),
            queue=Q(queue_name, auto_delete=True),
            routing_key="test",
            handle_message=callback,
        )
    ]

    consumer = BasicConsumer(rmq_url, listeners)
    publisher = BasicPublisher(rmq_url, exchange, "test")

    with run_consumer(consumer):
        response = publisher.rpc(b"aaa", timeout=None)
        assert isinstance(response, PikaMessage)
        assert response.body == b"aaa-reply"
        logger.info("Received first response: %s", response.body)

        response2 = publisher.rpc(b"bbb", timeout=None)
        assert isinstance(response2, PikaMessage)
        assert response2.body == b"bbb-reply"
        logger.info("Received second response: %s", response.body)


def test_publisher_with_direct_reply_timeout(rmq_url, run_consumer):
    exchange = "exchange_" + str(random.randint(100000, 999999))
    queue_name = "queue_" + str(random.randint(100000, 999999))

    def callback(msg: PikaMessage):
        sleep(2)
        return msg.body + b"-reply"

    listeners = [
        Listener(
            exchange=Exchange(type="topic", name=exchange, auto_delete=True,),
            queue=Q(queue_name, auto_delete=True),
            routing_key="test",
            handle_message=callback,
        )
    ]

    consumer = BasicConsumer(rmq_url, listeners)
    publisher = BasicPublisher(rmq_url, exchange, "test")

    with run_consumer(consumer):
        with pytest.raises(TimeoutError):
            publisher.rpc(b"yyy", timeout=1)


def test_publisher_with_direct_reply_timeout_but_message_replied_faster(
    rmq_url, run_consumer
):
    exchange = "exchange_" + str(random.randint(100000, 999999))
    queue_name = "queue_" + str(random.randint(100000, 999999))

    def callback(msg: PikaMessage):
        return msg.body + b"-reply"

    listeners = [
        Listener(
            exchange=Exchange(type="topic", name=exchange, auto_delete=True,),
            queue=Q(queue_name, auto_delete=True),
            routing_key="test",
            handle_message=callback,
        )
    ]

    consumer = BasicConsumer(rmq_url, listeners)
    publisher = BasicPublisher(rmq_url, exchange, "test")

    with run_consumer(consumer):
        response = publisher.rpc(b"xxx", timeout=10)
        assert response.body == b"xxx-reply"
