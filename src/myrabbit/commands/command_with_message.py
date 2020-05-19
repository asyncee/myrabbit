from dataclasses import dataclass
from typing import Generic
from typing import TypeVar

from myrabbit.commands.command_outcome import CommandOutcome
from myrabbit.commands.reply_headers import CommandReplyHeaders
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

    def is_success(self) -> bool:
        reply_outcome: str = self.message.properties.headers[
            CommandReplyHeaders.REPLY_OUTCOME
        ]
        return reply_outcome == CommandOutcome.SUCCESS.name

    def is_failure(self) -> bool:
        reply_outcome: str = self.message.properties.headers[
            CommandReplyHeaders.REPLY_OUTCOME
        ]
        return reply_outcome == CommandOutcome.FAILURE.name
