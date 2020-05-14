import queue
from dataclasses import dataclass
from typing import Callable

from pydantic import BaseModel

from myrabbit.commands.command_bus import CommandBus
from myrabbit.commands.command_bus_adapter import CommandBusAdapter
from myrabbit.commands.command_with_message import CommandWithMessage
from myrabbit.core.consumer.consumer import Consumer


# todo: commands must reply with Success or an Error automatically via headers
#   and may contain resulting body
# todo: test command that does not need reply
# todo: test command that needs a reply
# todo: test rpc command
# todo: test command with correlation or custom headers (think of sagas)
# todo: test that exactly one of multiple command handlers receives the message
# todo: test that command is requeued if failed
# todo: test exclusive command handler — only single handler can be online


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
