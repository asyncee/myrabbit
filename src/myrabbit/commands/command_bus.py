from functools import wraps
from typing import Callable
from typing import Optional

import pika
from pika import BasicProperties
from pika import URLParameters

from myrabbit.commands.command_with_message import CommandWithMessage
from myrabbit.core.consumer.listener import Exchange
from myrabbit.core.consumer.listener import Listener
from myrabbit.core.consumer.listener import Queue
from myrabbit.core.consumer.pika_message import PikaMessage
from myrabbit.core.publisher.publisher import Publisher
from myrabbit.core.serializer import JsonSerializer
from myrabbit.core.serializer import Serializer


class CommandBus:
    def __init__(
        self,
        amqp_url: str,
        serializer: Optional[Serializer] = None,
        default_exchange_params: Optional[dict] = None,
        default_queue_params: Optional[dict] = None,
    ):
        self._amqp_url = amqp_url
        self._serializer: Serializer = serializer or JsonSerializer()
        self.default_exchange_params = default_exchange_params or {}
        self.default_queue_params = default_queue_params or {}

        self._publisher_connection = pika.BlockingConnection(URLParameters(amqp_url))

    def publish(
        self,
        command_destination: str,
        command_name: str,
        body: Optional[dict] = None,
        properties: Optional[BasicProperties] = None,
    ) -> None:
        properties = properties or BasicProperties()

        body = body or {}
        content_type, binary_body = self._serializer.serialize(body)
        properties.content_type = content_type

        with Publisher(self._publisher_connection) as publisher:
            publisher.publish(
                self._exchange(command_destination),
                self._routing_key(command_name),
                binary_body,
                properties,
            )

    def listener(
        self,
        command_destination: str,
        command_name: str,
        callback: Callable[[CommandWithMessage], None],
        exchange_params: Optional[dict] = None,
        queue_params: Optional[dict] = None,
    ) -> Listener:
        queue_params = queue_params or {}
        queue_params = {**self.default_queue_params, **queue_params}
        queue_params.setdefault(
            "name", f"{command_destination}.{command_name}",
        )

        exchange_params = exchange_params or {}
        exchange_params = {**self.default_exchange_params, **exchange_params}
        exchange_params.setdefault("type", "direct")
        exchange_params.setdefault("name", self._exchange(command_destination))

        @wraps(callback)
        def deserialize_message(message: PikaMessage) -> None:
            callback(
                CommandWithMessage(self._serializer.deserialize(message.body), message)
            )

        return Listener(
            exchange=Exchange(**exchange_params),
            queue=Queue(**queue_params),
            routing_key=command_name,
            handle_message=deserialize_message,
        )

    def _exchange(self, command_destination: str) -> str:
        return f"{command_destination}.commands"

    def _routing_key(self, command_name: str) -> str:
        return command_name
