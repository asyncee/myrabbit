from dataclasses import dataclass
from typing import Generic
from typing import TypeVar

from myrabbit.core.consumer.pika_message import PikaMessage

CommandType = TypeVar("CommandType")
CommandReplyType = TypeVar("CommandReplyType")


@dataclass
class CommandWithMessage(Generic[CommandType]):
    command: CommandType
    message: PikaMessage


@dataclass
class ReplyWithMessage(Generic[CommandReplyType]):
    reply: CommandReplyType
    message: PikaMessage
