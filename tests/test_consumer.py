import logging
import random
from queue import Queue
from time import sleep

from myrabbit.core.consumer.basic_consumer import BasicConsumer
from myrabbit.core.consumer.handle_message_strategy import ManualHandle
from myrabbit.core.consumer.listener import Exchange
from myrabbit.core.consumer.listener import Listener
from myrabbit.core.consumer.listener import Queue as Q
from myrabbit.core.consumer.pika_message import PikaMessage
from myrabbit.core.publisher.basic_publisher import BasicPublisher

logger = logging.getLogger(__name__)


def test_basic_consumer_acknowledge(rmq_url, run_consumer):
    queue = Queue()

    def callback(msg: PikaMessage):
        queue.put(msg)
        msg.acknowledge()

    exchange = "exchange_" + str(random.randint(100000, 999999))
    queue_name = "queue_" + str(random.randint(100000, 999999))

    listeners = [
        Listener(
            exchange=Exchange(type="topic", name=exchange, auto_delete=True,),
            queue=Q(queue_name, auto_delete=True),
            routing_key="test",
            handle_message=callback,
            handle_message_strategy=ManualHandle(),
        )
    ]

    consumer = BasicConsumer(rmq_url, listeners)

    publisher = BasicPublisher(rmq_url, exchange, "test")

    with run_consumer(consumer):
        publisher.publish(b"test-message")
        message = queue.get(timeout=1)

    assert message.body == b"test-message"


def test_basic_consumer_requeue(rmq_url, run_consumer):
    queue = Queue()
    retries = 0

    def callback(msg: PikaMessage):
        nonlocal retries
        queue.put(msg)
        retries += 1

        if retries == 3:
            msg.acknowledge()
        else:
            msg.requeue()

    exchange = "exchange_" + str(random.randint(100000, 999999))
    queue_name = "queue_" + str(random.randint(100000, 999999))

    listeners = [
        Listener(
            exchange=Exchange(type="topic", name=exchange, auto_delete=True,),
            queue=Q(queue_name, auto_delete=True),
            routing_key="test",
            handle_message=callback,
            handle_message_strategy=ManualHandle(),
        )
    ]

    consumer = BasicConsumer(rmq_url, listeners)

    publisher = BasicPublisher(rmq_url, exchange, "test")

    with run_consumer(consumer):
        publisher.publish(b"test-message")

        while queue.qsize() != 3:
            sleep(0.1)

    message1 = queue.get()
    message2 = queue.get()
    message3 = queue.get()
    assert message1.body == message2.body == message3.body == b"test-message"


def test_basic_consumer_did_not_acknowledged(rmq_url, run_consumer):
    queue = Queue()

    def callback_no_ack(msg: PikaMessage):
        queue.put(msg)

    def callback_ack(msg: PikaMessage):
        msg.acknowledge()
        queue.put(msg)

    exchange = "exchange_" + str(random.randint(100000, 999999))
    queue_name = "queue_" + str(random.randint(100000, 999999))

    listeners_no_ack = [
        Listener(
            exchange=Exchange(type="topic", name=exchange, auto_delete=False),
            queue=Q(queue_name, auto_delete=False),
            routing_key="test",
            handle_message=callback_no_ack,
            handle_message_strategy=ManualHandle(),
        )
    ]
    listeners_ack = [
        Listener(
            exchange=Exchange(type="topic", name=exchange, auto_delete=False),
            queue=Q(queue_name, auto_delete=False),
            routing_key="test",
            handle_message=callback_ack,
            handle_message_strategy=ManualHandle(),
        )
    ]

    noack_consumer = BasicConsumer(rmq_url, listeners_no_ack)
    ack_consumer = BasicConsumer(rmq_url, listeners_ack)

    publisher = BasicPublisher(rmq_url, exchange, "test")

    with run_consumer(noack_consumer):
        # This consumer does not acknowledge message.
        publisher.publish(b"test-message")

    with run_consumer(ack_consumer):
        # This consumer does.
        sleep(1)

    assert queue.qsize() == 2
    message1 = queue.get()
    message2 = queue.get()
    assert message1.body == message2.body == b"test-message"
