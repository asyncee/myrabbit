from typing import Optional

from pika import BasicProperties

from myrabbit.core.publisher.basic_publisher import BasicPublisher


class Publisher:
    def __init__(self, amqp_url: str):
        self._amqp_url = amqp_url

    def publish(
        self,
        exchange: str,
        routing_key: str,
        message: bytes,
        properties: Optional[BasicProperties] = None,
    ):
        basic_publisher = BasicPublisher(self._amqp_url, exchange, routing_key)
        basic_publisher.publish(message, properties)
