import queue
from dataclasses import dataclass
from typing import Callable

from myrabbit import EventWithMessage
from myrabbit.commands.command_with_message import CommandWithMessage
from myrabbit.commands.command_with_message import ReplyWithMessage
from myrabbit.core.consumer.consumer import Consumer
from myrabbit.service import Service


@dataclass
class YEvent:
    name: str


@dataclass
class Command:
    name: str


@dataclass
class CommandReply:
    reply_name: str


def test_service_events(
    make_service: Callable, run_consumer: Callable, rmq_url: str
) -> None:
    q: queue.Queue = queue.Queue()

    x: Service = make_service("X")
    y: Service = make_service("Y")

    @x.on_event("Y", YEvent)
    def handle_y_event(event: EventWithMessage[YEvent]) -> None:
        q.put(event)

    consumer = Consumer(rmq_url, x.listeners)
    with run_consumer(consumer):
        y.publish(YEvent(name="y-service-event"))

        message: EventWithMessage = q.get(block=True, timeout=1)
        assert message.event == YEvent(name="y-service-event")


def test_service_commands(
    make_service: Callable, run_consumer: Callable, rmq_url: str
) -> None:
    q: queue.Queue = queue.Queue()

    x: Service = make_service("X")
    y: Service = make_service("Y")

    @x.on_command(Command)
    def x_handle_command(command: CommandWithMessage[Command]) -> None:
        q.put(command)

    consumer = Consumer(rmq_url, x.listeners)
    with run_consumer(consumer):
        y.send("X", Command(name="command-for-x"))

        message: CommandWithMessage = q.get(block=True, timeout=1)
        assert message.command == Command(name="command-for-x")


def test_service_commands_reply_dataclass(
    make_service: Callable, run_consumer: Callable, rmq_url: str
) -> None:
    q: queue.Queue = queue.Queue()

    x: Service = make_service("X")
    y: Service = make_service("Y")

    @x.on_command(Command)
    def x_handle_command(command: CommandWithMessage[Command]) -> CommandReply:
        return CommandReply(reply_name="reply-from-x")

    @y.on_command_reply("X", Command, CommandReply)
    def on_reply_from_x(reply: ReplyWithMessage[Command]) -> None:
        q.put(reply)

    consumer = Consumer(rmq_url, x.listeners + y.listeners)
    with run_consumer(consumer):
        y.send("X", Command(name="test"))

        message: ReplyWithMessage = q.get(block=True, timeout=1)
        assert message.reply == CommandReply("reply-from-x")


def test_service_commands_reply_string(
    make_service: Callable, run_consumer: Callable, rmq_url: str
) -> None:
    q: queue.Queue = queue.Queue()

    x: Service = make_service("X")
    y: Service = make_service("Y")

    @x.on_command(Command)
    def x_handle_command(command: CommandWithMessage[Command]) -> str:
        return "string-reply"

    @y.on_command_reply("X", Command)
    def on_reply_from_x(reply: ReplyWithMessage[Command]) -> None:
        q.put(reply)

    consumer = Consumer(rmq_url, x.listeners + y.listeners)
    with run_consumer(consumer):
        y.send("X", Command(name="test"))

        message: ReplyWithMessage = q.get(block=True, timeout=1)
        assert message.reply == "string-reply"


def test_service_commands_reply_custom_queue(
    make_service: Callable, run_consumer: Callable, rmq_url: str
) -> None:
    q: queue.Queue = queue.Queue()

    x: Service = make_service("X")
    y: Service = make_service("Y")

    @x.on_command(Command)
    def x_handle_command(command: CommandWithMessage[Command]) -> str:
        return "another-reply-from-x"

    @y.on_command_reply("X", Command, listen_on="my-custom-reply-queue")
    def on_reply_from_x(reply: ReplyWithMessage[Command]) -> None:
        q.put(reply)

    consumer = Consumer(rmq_url, x.listeners + y.listeners)
    with run_consumer(consumer):
        y.send("X", Command(name="test"), reply_to="my-custom-reply-queue")

        message: ReplyWithMessage = q.get(block=True, timeout=1)
        assert message.reply == "another-reply-from-x"
