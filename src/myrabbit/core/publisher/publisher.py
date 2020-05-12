from __future__ import annotations

from contextlib import contextmanager
from typing import Optional

import pika
from pika import BasicProperties
from pika import URLParameters

from myrabbit.core.consumer.pika_message import PikaMessage
from myrabbit.core.publisher.rpc import rpc


class Publisher:
    def __init__(self, connection: pika.BlockingConnection):
        self._connection = connection
        self._channel = self._connection.channel()

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

    def close(self) -> None:
        self._channel.close()

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

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


@contextmanager
def make_publisher(amqp_url: str):
    with pika.BlockingConnection(URLParameters(amqp_url)) as conn:
        with Publisher(conn) as p:
            yield p
