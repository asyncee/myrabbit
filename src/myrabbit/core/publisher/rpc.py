import logging
import threading
from typing import Optional

import pika
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.channel import Channel
from pika.exceptions import ChannelWrongStateError
from pika.exceptions import ConnectionWrongStateError
from pika.spec import Basic

from myrabbit.core.consumer.pika_message import PikaMessage

LOGGER = logging.getLogger(__name__)


def _timeout_timer(
    chan: Channel,
    wait_secs: int,
    stop_timer_event: threading.Event,
    timeout_event: threading.Event,
) -> None:
    # Lets current thread to block while waiting for timeout.
    blocker = threading.Event()

    # Wakeup ten times to check if timer is still needed to run.
    sleep_time = wait_secs / 10
    for _ in range(10):
        blocker.wait(timeout=sleep_time)
        if stop_timer_event.is_set():
            return

    def run_threadsafe() -> None:
        try:
            # https://www.rabbitmq.com/amqp-0-9-1-reference.html#constants
            internal_error = 541
            chan.close(
                reply_code=internal_error,
                reply_text=f"RPC wait timeout reached ({wait_secs} s)",
            )
        except (ChannelWrongStateError, ConnectionWrongStateError):
            LOGGER.debug("Rpc call aborted because of timeout (%s s)", wait_secs)
        except Exception:
            LOGGER.exception("Exception happened during rpc call timeout handler")
        else:
            LOGGER.debug("Rpc call aborted due to timeout (%s s)", wait_secs)

        timeout_event.set()

    try:
        chan.connection.add_callback_threadsafe(run_threadsafe)
    except ConnectionWrongStateError:
        pass


def rpc(
    connection: pika.BlockingConnection,
    exchange_name: str,
    routing_key: str,
    body: bytes,
    properties: Optional[BasicProperties] = None,
    timeout: Optional[int] = None,
) -> PikaMessage:
    response = None
    timeout_event = threading.Event()
    stop_timer_event = threading.Event()

    def on_reply_received(
        reply_channel: Channel,
        reply_method_frame: Basic.Deliver,
        reply_properties: pika.BasicProperties,
        reply_body: bytes,
    ) -> None:
        nonlocal response
        response = PikaMessage(
            reply_channel, reply_method_frame, reply_properties, reply_body
        )
        reply_channel.close()
        stop_timer_event.set()

    direct_reply_queue = "amq.rabbitmq.reply-to"
    channel: BlockingChannel

    with connection.channel() as channel:
        channel.basic_consume(direct_reply_queue, on_reply_received, auto_ack=True)

        props = properties or BasicProperties()
        props.reply_to = direct_reply_queue

        channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=body,
            properties=props,
        )

        if timeout:
            timer = threading.Thread(
                target=_timeout_timer,
                args=(channel, timeout, stop_timer_event, timeout_event),
                daemon=True,
            )
            timer.start()

        channel.start_consuming()

    if timeout_event.is_set():
        raise TimeoutError

    assert isinstance(response, PikaMessage)
    return response
