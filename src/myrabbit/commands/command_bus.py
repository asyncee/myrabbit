import logging
import uuid
from functools import wraps
from typing import Any
from typing import Callable
from typing import Optional
from typing import Union

import pika
from pika import BasicProperties
from pika import URLParameters

from myrabbit.commands import command_outcome
from myrabbit.commands.command_outcome import CommandReply
from myrabbit.commands.command_with_message import CommandWithMessage
from myrabbit.commands.command_with_message import ReplyWithMessage
from myrabbit.core.consumer.listener import Exchange
from myrabbit.core.consumer.listener import Listener
from myrabbit.core.consumer.listener import Queue
from myrabbit.core.consumer.pika_message import PikaMessage
from myrabbit.core.consumer.reply import Reply
from myrabbit.core.publisher.publisher import Publisher
from myrabbit.core.serializer import JsonSerializer
from myrabbit.core.serializer import Serializer

logger = logging.getLogger(__name__)


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

    def send(
        self,
        command_sender: str,
        command_destination: str,
        command_name: str,
        body: Optional[dict] = None,
        properties: Optional[BasicProperties] = None,
    ) -> None:
        properties = properties or BasicProperties()

        body = body or {}
        content_type, binary_body = self._serializer.serialize(body)

        properties.content_type = content_type

        if properties.reply_to is None:
            properties.reply_to = self._default_reply_queue_name(
                command_sender, command_destination, command_name
            )

        if properties.correlation_id is None:
            properties.correlation_id = uuid.uuid4().hex

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
        callback: Callable[[CommandWithMessage], Optional[Union[CommandReply, Any]]],
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
        def deserialize_command_and_handle_reply(
            message: PikaMessage,
        ) -> Optional[Reply]:
            try:
                callback_result: Optional[Union[CommandReply, Any]] = callback(
                    CommandWithMessage(
                        self._serializer.deserialize(message.body), message
                    )
                )
            except Exception as e:
                logger.exception(
                    "Exception happened during handling message %s using handler %s",
                    message,
                    callback,
                )
                return command_outcome.exception(e).to_reply(self._serializer)

            if callback_result is None:
                return None

            if isinstance(callback_result, CommandReply):
                return callback_result.to_reply(self._serializer)

            return command_outcome.success(body=callback_result).to_reply(
                self._serializer
            )

        return Listener(
            exchange=Exchange(**exchange_params),
            queue=Queue(**queue_params),
            routing_key=self._routing_key(command_name),
            handle_message=deserialize_command_and_handle_reply,
        )

    def reply_listener(
        self,
        command_sender: str,
        command_destination: str,
        command_name: str,
        callback: Callable[[ReplyWithMessage], None],
        exchange_params: Optional[dict] = None,
        queue_params: Optional[dict] = None,
    ) -> Listener:
        queue_params = queue_params or {}
        queue_params = {**self.default_queue_params, **queue_params}
        queue_params.setdefault(
            "name",
            self._default_reply_queue_name(
                command_sender, command_destination, command_name
            ),
        )
        routing_key = queue_params["name"]  # Exchange is direct.

        exchange_params = exchange_params or {}
        exchange_params = {**self.default_exchange_params, **exchange_params}
        exchange_params.setdefault("type", "direct")
        exchange_params.setdefault("name", self._exchange(command_destination))

        @wraps(callback)
        def deserialize_message(message: PikaMessage) -> None:
            reply: dict = self._serializer.deserialize(message.body)
            callback(ReplyWithMessage(reply=reply, message=message))

        return Listener(
            exchange=Exchange(**exchange_params),
            queue=Queue(**queue_params),
            routing_key=routing_key,
            handle_message=deserialize_message,
        )

    def _exchange(self, command_destination: str) -> str:
        return f"{command_destination}.commands"

    def _routing_key(self, command_name: str) -> str:
        return command_name

    def _default_reply_queue_name(
        self, command_sender: str, command_destination: str, command_name: str
    ) -> str:
        return (
            f"{command_sender}.listen-reply-from:{command_destination}.{command_name}"
        )
