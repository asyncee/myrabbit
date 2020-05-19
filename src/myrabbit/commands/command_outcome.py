import traceback
from dataclasses import dataclass
from enum import Enum
from typing import Any
from typing import Optional

import pika

from myrabbit.core.consumer.reply import Reply
from myrabbit.commands.reply_headers import CommandReplyHeaders
from myrabbit.core.serializer import Serializer


@dataclass
class CommandReply:
    body: Any
    properties: Optional[pika.BasicProperties] = None

    def to_reply(self, serializer: Serializer) -> Reply:
        content_type, body = serializer.serialize(self.body)
        properties = self.properties
        return Reply(body=body, properties=properties)


@dataclass
class Success:
    pass


@dataclass
class Failure:
    pass


class CommandOutcome(Enum):
    SUCCESS = Success()
    FAILURE = Failure()


@dataclass
class ExceptionInfo:
    type: str
    message: str
    traceback: str


def make_command_reply(
    body: Any, outcome: CommandOutcome, properties: pika.BasicProperties
) -> CommandReply:
    properties = properties or pika.BasicProperties()

    headers = {
        CommandReplyHeaders.REPLY_OUTCOME: outcome.name,
        CommandReplyHeaders.REPLY_TYPE: body.__class__.__name__,
    }
    properties.headers = properties.headers or {}
    properties.headers.update(headers)
    return CommandReply(body=body, properties=properties)


def success(
    body: Any = CommandOutcome.SUCCESS.value,
    properties: Optional[pika.BasicProperties] = None,
) -> CommandReply:
    return make_command_reply(body, CommandOutcome.SUCCESS, properties)


def failure(
    body: Any = CommandOutcome.FAILURE.value,
    properties: Optional[pika.BasicProperties] = None,
) -> CommandReply:
    return make_command_reply(body, CommandOutcome.FAILURE, properties)


def exception(exc: Exception) -> CommandReply:
    return failure(
        ExceptionInfo(
            type=type(exc).__name__, message=str(exc), traceback=traceback.format_exc(),
        )
    )
