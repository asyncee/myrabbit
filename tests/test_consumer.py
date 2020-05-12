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
from myrabbit.core.publisher.publisher import make_publisher

logger = logging.getLogger(__name__)


def test_basic_consumer_acknowledge(rmq_url, run_consumer):
    queue: Queue = Queue()

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

    with run_consumer(consumer), make_publisher(rmq_url) as publisher:
        publisher.publish(exchange, "test", b"test-message")
        message = queue.get(timeout=1)

    assert message.body == b"test-message"


def test_basic_consumer_requeue(rmq_url, run_consumer):
    queue: Queue = Queue()
    retries = 0

    def callback(msg: PikaMessage) -> None:
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
            exchange=Exchange(type="topic", name=exchange, auto_delete=True),
            queue=Q(queue_name, auto_delete=True),
            routing_key="test",
            handle_message=callback,
            handle_message_strategy=ManualHandle(),
        )
    ]

    consumer = BasicConsumer(rmq_url, listeners)

    with run_consumer(consumer), make_publisher(rmq_url) as publisher:
        publisher.publish(exchange, "test", b"test-message")

        while queue.qsize() != 3:
            sleep(0.1)

    message1 = queue.get()
    message2 = queue.get()
    message3 = queue.get()
    assert message1.body == message2.body == message3.body == b"test-message"


def test_basic_consumer_did_not_acknowledged(rmq_url, run_consumer) -> None:
    queue: Queue = Queue()

    def callback_no_ack(msg: PikaMessage) -> None:
        queue.put(msg)

    def callback_ack(msg: PikaMessage) -> None:
        msg.acknowledge()
        queue.put(msg)

    exchange = "myrabbit_test_exchange"
    queue_name = "test_basic_consumer_did_not_acknowledged"

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

    # This consumer does not acknowledge message.
    with run_consumer(noack_consumer), make_publisher(rmq_url) as publisher:
        publisher.publish(exchange, "test", b"test-message")

    # This consumer does.
    with run_consumer(ack_consumer):
        sleep(1)

    assert queue.qsize() == 2
    message1 = queue.get()
    message2 = queue.get()
    assert message1.body == message2.body == b"test-message"
