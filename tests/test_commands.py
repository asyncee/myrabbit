import queue
from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import Dict
from typing import Tuple

import pika
import pytest
from pydantic import BaseModel

from myrabbit.commands.command_bus import CommandBus
from myrabbit.commands.command_bus_adapter import CommandBusAdapter
from myrabbit.commands.command_outcome import CommandOutcome
from myrabbit.commands.command_outcome import failure
from myrabbit.commands.command_outcome import success
from myrabbit.commands.command_with_message import CommandWithMessage
from myrabbit.commands.command_with_message import ReplyWithMessage
from myrabbit.commands.reply_headers import CommandReplyHeaders
from myrabbit.core.consumer.consumer import Consumer


def test_command_bus(
    rmq_url: str, run_consumer: Callable, command_bus: CommandBus
) -> None:
    q: queue.Queue = queue.Queue()

    def callback(msg: CommandWithMessage) -> None:
        q.put(msg)

    listeners = [
        command_bus.listener("payments", "CapturePayment", callback),
    ]

    consumer = Consumer(rmq_url, listeners)

    with run_consumer(consumer):
        command_bus.send("payments", "CapturePayment", {"payment_id": 1})

        message = q.get(block=True, timeout=1)
        assert isinstance(message, CommandWithMessage)
        assert message.command == {"payment_id": 1}


def test_command_bus_reply_listener(
    rmq_url: str, run_consumer: Callable, command_bus: CommandBus
) -> None:
    q: queue.Queue = queue.Queue()

    def callback(msg: CommandWithMessage[dict]) -> Any:
        q.put(msg)
        return msg.command["this-message-will-be-sent-as-reply"]

    def reply_callback(msg: ReplyWithMessage) -> None:
        q.put(msg)

    listeners = [
        command_bus.listener("replies-1", "reply_to_me", callback),
        command_bus.reply_listener("replies-1", "reply_to_me", reply_callback),
    ]

    consumer = Consumer(rmq_url, listeners)

    with run_consumer(consumer):
        command_bus.send(
            "replies-1",
            "reply_to_me",
            {"this-message-will-be-sent-as-reply": "this-is-reply"},
        )

        message = q.get(block=True, timeout=1)
        assert isinstance(message, CommandWithMessage)
        assert message.command == {
            "this-message-will-be-sent-as-reply": "this-is-reply"
        }

        message = q.get(block=True, timeout=1)
        assert isinstance(message, ReplyWithMessage)
        assert message.reply == "this-is-reply"

        headers = message.message.properties.headers
        assert headers[CommandReplyHeaders.REPLY_OUTCOME] == CommandOutcome.SUCCESS.name
        assert headers[CommandReplyHeaders.REPLY_TYPE] == "str"
        assert message.is_success() is True
        assert message.is_failure() is False


def test_command_bus_custom_reply_queue(
    rmq_url: str, run_consumer: Callable, command_bus: CommandBus
) -> None:
    dest = "test-custom-reply-queue"
    reply_dest = dest + "-reply"
    q: queue.Queue = queue.Queue()

    def send_reply(msg: CommandWithMessage[dict]) -> Any:
        q.put(msg)
        return "response"

    def on_reply(msg: ReplyWithMessage) -> None:
        q.put(msg)

    listeners = [
        command_bus.listener(dest, "command", send_reply),
        command_bus.reply_listener(
            dest, "command", on_reply, queue_params={"name": reply_dest}
        ),
    ]

    consumer = Consumer(rmq_url, listeners)

    with run_consumer(consumer):
        command_bus.send(
            dest,
            "command",
            {"body": True},
            properties=pika.BasicProperties(reply_to=reply_dest),
        )

        message = q.get(block=True, timeout=1)
        assert isinstance(message, CommandWithMessage)
        assert message.command == {"body": True}

        message = q.get(block=True, timeout=1)
        assert isinstance(message, ReplyWithMessage)
        assert message.reply == "response"


def test_command_bus_reply_types(
    rmq_url: str, run_consumer: Callable, command_bus: CommandBus
) -> None:
    q: queue.Queue = queue.Queue()

    # Reply type: (Reply sent, Reply received, Reply outcome, Reply type)
    replies: Dict[str, Tuple] = {
        "str": ("a string", "a string", CommandOutcome.SUCCESS.name, "str"),
        "dict": (
            {"this-is": "dict"},
            {"this-is": "dict"},
            CommandOutcome.SUCCESS.name,
            "dict",
        ),
        "int": (1, 1, CommandOutcome.SUCCESS.name, "int"),
        "float": (1.0, 1.0, CommandOutcome.SUCCESS.name, "float"),
        "boolean": (True, True, CommandOutcome.SUCCESS.name, "bool"),
        "empty_success": (success(), {}, CommandOutcome.SUCCESS.name, "Success"),
        "empty_failure": (failure(), {}, CommandOutcome.FAILURE.name, "Failure"),
        "success": (
            success(body={"some": "data"}),
            {"some": "data"},
            CommandOutcome.SUCCESS.name,
            "dict",
        ),
        "failure": (failure(body="test"), "test", CommandOutcome.FAILURE.name, "str"),
    }

    def send_reply(msg: CommandWithMessage[dict]) -> Any:
        response, _, _, _ = replies[msg.command["reply_type"]]
        return response

    def reply_callback(msg: ReplyWithMessage) -> None:
        q.put(msg)

    listeners = [
        command_bus.listener("replies", "reply_to_me", send_reply),
        command_bus.reply_listener("replies", "reply_to_me", reply_callback),
    ]

    consumer = Consumer(rmq_url, listeners)

    with run_consumer(consumer):
        for reply_type, expectation in replies.items():
            command_bus.send("replies", "reply_to_me", {"reply_type": reply_type})
            _, received, outcome, type_ = expectation

            message = q.get(block=True, timeout=1)
            assert isinstance(message, ReplyWithMessage)
            assert message.reply == received
            headers = message.message.properties.headers
            assert headers[CommandReplyHeaders.REPLY_OUTCOME] == outcome
            assert headers[CommandReplyHeaders.REPLY_TYPE] == type_
            if outcome == CommandOutcome.SUCCESS.name:
                assert message.is_success()
            elif outcome == CommandOutcome.FAILURE.name:
                assert message.is_failure()
            else:
                raise ValueError


def test_command_adapter(
    rmq_url: str, run_consumer: Callable, command_bus_adapter: CommandBusAdapter
) -> None:
    q: queue.Queue = queue.Queue()

    @dataclass
    class DataclassCommand:
        name: str

    class PydanticCommand(BaseModel):
        name: str

    def callback(msg: CommandWithMessage) -> None:
        q.put(msg)

    listeners = [
        command_bus_adapter.listener(
            "test_command_adapter", DataclassCommand, callback
        ),
        command_bus_adapter.listener("test_command_adapter", PydanticCommand, callback),
    ]

    consumer = Consumer(rmq_url, listeners)

    dataclass_command = DataclassCommand(name="dataclass-command")
    pydantic_command = PydanticCommand(name="pydantic-command")

    with run_consumer(consumer):
        command_bus_adapter.send("test_command_adapter", dataclass_command)
        command_bus_adapter.send("test_command_adapter", pydantic_command)

        message = q.get(block=True, timeout=1)
        assert isinstance(message.command, (DataclassCommand, PydanticCommand))
        assert message.command in (dataclass_command, pydantic_command)

        message2 = q.get(block=True, timeout=1)
        assert isinstance(message2.command, (DataclassCommand, PydanticCommand))
        assert type(message2.command) != type(message.command)
        assert message2.command in (dataclass_command, pydantic_command)
        assert message2.command != message.command


@pytest.mark.skip("not implemented")
def test_command_without_reply():
    raise NotImplementedError


@pytest.mark.skip("not implemented")
def test_rpc_command():
    raise NotImplementedError


@pytest.mark.skip("not implemented")
def test_command_custom_headers():
    # Test that it is possible to send custom headers with command
    # and to receive them back + some new reply headers.
    # Headers sent with command must be automatically added to reply
    # and marked as command headers.
    raise NotImplementedError


@pytest.mark.skip("not implemented")
def test_command_correlation_id():
    raise NotImplementedError


@pytest.mark.skip("not implemented")
def test_exactly_on_of_multiple_command_handlers_receives_the_message():
    raise NotImplementedError


@pytest.mark.skip("not implemented")
def test_command_is_requeued_if_failed():
    raise NotImplementedError


@pytest.mark.skip("not implemented")
def test_exclusive_command_handler():
    # Test that only single handler (consumer) can be online for
    # exclusive command.
    raise NotImplementedError
