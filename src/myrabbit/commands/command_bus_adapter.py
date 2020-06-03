from functools import wraps
from typing import Any, Callable, List, Optional, Type, Union

from pika import BasicProperties

from myrabbit.commands.command_bus import CommandBus
from myrabbit.commands.command_outcome import CommandReply
from myrabbit.commands.command_with_message import (
    CommandReplyType,
    CommandType,
    CommandWithMessage,
    ReplyWithMessage,
)
from myrabbit.core.consumer.listener import Listener
from myrabbit.core.converter import DEFAULT_CONVERTERS, Converter


class CommandBusAdapter:
    def __init__(
        self, command_bus: CommandBus, converters: Optional[List[Converter]] = None,
    ):
        self.command_bus = command_bus
        self.converters = converters or DEFAULT_CONVERTERS

    def get_converter(self, command: CommandType) -> Converter:
        for converter in self.converters:
            if converter.accepts(command):
                return converter
        raise RuntimeError("Converter not found")

    def send(
        self,
        command_sender: str,
        command_destination: str,
        command: CommandType,
        properties: Optional[BasicProperties] = None,
        reply_headers: Optional[dict] = None,
    ) -> None:
        command_name, body = self.get_converter(command).name_and_body(command)
        self.command_bus.send(
            command_sender,
            command_destination,
            command_name,
            body,
            properties,
            reply_headers,
        )

    def listener(
        self,
        command_destination: str,
        command_type: Type[CommandType],
        callback: Callable[[CommandWithMessage], Optional[Union[CommandReply, Any]]],
        exchange_params: Optional[dict] = None,
        queue_params: Optional[dict] = None,
    ) -> Listener:
        converter = self.get_converter(command_type)

        @wraps(callback)
        def instantiate_command(
            command_with_message: CommandWithMessage,
        ) -> Optional[Union[CommandReply, Any]]:
            command_with_message.command = converter.instantiate(
                command_type, command_with_message.command
            )
            return callback(command_with_message)

        command_name = converter.name(command_type)

        return self.command_bus.listener(
            command_destination=command_destination,
            command_name=command_name,
            callback=instantiate_command,
            exchange_params=exchange_params,
            queue_params=queue_params,
        )

    def reply_listener(
        self,
        command_sender: str,
        command_destination: str,
        command_type: Type[CommandType],
        callback: Callable[[ReplyWithMessage], None],
        reply_type: Optional[Type[CommandReplyType]] = None,
        exchange_params: Optional[dict] = None,
        queue_params: Optional[dict] = None,
    ) -> Listener:
        @wraps(callback)
        def instantiate_reply(reply: ReplyWithMessage) -> None:
            if reply_type is not None:
                converter = self.get_converter(reply_type)
                reply.reply = converter.instantiate(reply_type, reply.reply)
            callback(reply)

        command_name = self.get_converter(command_type).name(command_type)

        return self.command_bus.reply_listener(
            command_sender=command_sender,
            command_destination=command_destination,
            command_name=command_name,
            callback=instantiate_reply,
            exchange_params=exchange_params,
            queue_params=queue_params,
        )
