from functools import wraps
from typing import Callable
from typing import List
from typing import Optional
from typing import Type

from pika import BasicProperties

from myrabbit.commands.command_bus import CommandBus
from myrabbit.commands.command_with_message import CommandType
from myrabbit.commands.command_with_message import CommandWithMessage
from myrabbit.core.consumer.listener import Listener
from myrabbit.core.converter import Converter
from myrabbit.core.converter import DEFAULT_CONVERTERS


class CommandBusAdapter:
    def __init__(
        self, command_bus: CommandBus, converters: Optional[List[Converter]] = None,
    ):
        self.command_bus = command_bus
        self.converters = converters or DEFAULT_CONVERTERS

    def _converter(self, command: CommandType) -> Converter:
        for converter in self.converters:
            if converter.accepts(command):
                return converter
        raise RuntimeError("Converter not found")

    def send(
        self,
        command_destination: str,
        command: CommandType,
        properties: Optional[BasicProperties] = None,
    ) -> None:
        command_name, body = self._converter(command).name_and_body(command)
        self.command_bus.send(command_destination, command_name, body, properties)

    def listener(
        self,
        command_destination: str,
        command_type: Type[CommandType],
        callback: Callable[[CommandWithMessage], None],
        exchange_params: Optional[dict] = None,
        queue_params: Optional[dict] = None,
    ) -> Listener:
        converter = self._converter(command_type)

        @wraps(callback)
        def instantiate_command(command_with_message: CommandWithMessage) -> None:
            command_with_message.command = converter.instantiate(
                command_type, command_with_message.command
            )
            callback(command_with_message)

        command_name = converter.name(command_type)

        return self.command_bus.listener(
            command_destination=command_destination,
            command_name=command_name,
            callback=instantiate_command,
            exchange_params=exchange_params,
            queue_params=queue_params,
        )
