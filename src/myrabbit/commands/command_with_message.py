from dataclasses import dataclass
from typing import Generic
from typing import TypeVar

from myrabbit.core.consumer.pika_message import PikaMessage

CommandType = TypeVar("CommandType")


@dataclass
class CommandWithMessage(Generic[CommandType]):
    command: CommandType
    message: PikaMessage
