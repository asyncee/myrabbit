import logging
import uuid
from functools import wraps
from typing import Any, Callable, Optional, Union

from pika import BasicProperties

from myrabbit.commands import command_outcome
from myrabbit.commands.command_outcome import CommandReply
from myrabbit.commands.command_with_message import (CommandWithMessage,
                                                    ReplyWithMessage)
from myrabbit.core.consumer.callbacks import Callbacks
from myrabbit.core.consumer.listener import Exchange, Listener, Queue
from myrabbit.core.consumer.pika_message import PikaMessage
from myrabbit.core.consumer.reply import Reply
from myrabbit.core.publisher.reconnecting_publisher import \
    ReconnectingPublisherFactory
from myrabbit.core.serializer import JsonSerializer, Serializer

logger = logging.getLogger(__name__)


class CommandBus:
    def __init__(
        self,
        amqp_url: str,
        serializer: Optional[Serializer] = None,
        default_exchange_params: Optional[dict] = None,
        default_queue_params: Optional[dict] = None,
        callbacks: Optional[Callbacks] = None,
    ):
        self._amqp_url = amqp_url
        self._serializer: Serializer = serializer or JsonSerializer()
        self.default_exchange_params = default_exchange_params or {}
        self.default_queue_params = default_queue_params or {}
        self._publisher_factory = ReconnectingPublisherFactory(self._amqp_url)
        self._callbacks = callbacks

    def set_callbacks(self, callbacks: Callbacks) -> None:
        self._callbacks = callbacks

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

        with self._publisher_factory.publisher() as publisher:
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
            callbacks=self._callbacks,
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
            callbacks=self._callbacks,
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
