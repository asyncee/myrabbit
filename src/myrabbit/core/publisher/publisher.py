from __future__ import annotations

import logging
from contextlib import contextmanager
from functools import wraps
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Generator
from typing import Optional
from typing import Type

import pika
from pika import BasicProperties
from pika import URLParameters
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import ChannelClosedByBroker

from myrabbit.core.consumer.pika_message import PikaMessage
from myrabbit.core.publisher.rpc import rpc

logger = logging.getLogger(__name__)


def ignore_missing_exchange(fn: Callable) -> Callable:
    @wraps(fn)
    def handle(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except ChannelClosedByBroker as e:
            if int(e.reply_code) == 404:
                logger.error(
                    "Can not publish message because exchange does not exist: %s %s",
                    e.reply_code,
                    e.reply_text,
                )
            else:
                raise

    return handle


class Publisher:
    def __init__(self, connection: pika.BlockingConnection):
        self._connection = connection
        self._channel: BlockingChannel = self._connection.channel()

    @ignore_missing_exchange
    def publish(
        self,
        exchange: str,
        routing_key: str,
        message: bytes,
        properties: Optional[BasicProperties] = None,
    ) -> None:
        self._channel.basic_publish(
            exchange, routing_key, message, properties,
        )

    @ignore_missing_exchange
    def close(self) -> None:
        self._channel.close()
        self._connection.close()

    def rpc(
        self,
        exchange: str,
        routing_key: str,
        message: bytes,
        properties: Optional[BasicProperties] = None,
        timeout: Optional[int] = 1,
    ) -> PikaMessage:
        return rpc(
            self._connection, exchange, routing_key, message, properties, timeout
        )

    def __enter__(self) -> Publisher:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[Exception]],
        exc_val: Optional[Any],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()


@contextmanager
def make_publisher(amqp_url: str) -> Generator[Publisher, None, None]:
    with pika.BlockingConnection(URLParameters(amqp_url)) as conn:
        with Publisher(conn) as p:
            yield p
