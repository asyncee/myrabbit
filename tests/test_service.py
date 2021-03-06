import queue
from dataclasses import dataclass
from typing import Callable

import pytest

from myrabbit import EventWithMessage
from myrabbit.commands.command_with_message import CommandWithMessage, ReplyWithMessage
from myrabbit.core.consumer.consumer import Consumer
from myrabbit.core.consumer.listener import Listener
from myrabbit.core.consumer.pika_message import PikaMessage
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


def test_service_callbacks(
    make_service: Callable, run_consumer: Callable, rmq_url: str
) -> None:
    q: queue.Queue = queue.Queue()

    x: Service = make_service("X")
    y: Service = make_service("Y")

    @x.before_request
    def x_before_request(listener: Listener, message: PikaMessage) -> None:
        q.put("x_before_request")

    @x.after_request
    def x_after_request(listener: Listener, message: PikaMessage) -> None:
        q.put("x_after_request")

    @x.on_event("Y", YEvent)
    def handle_y_event(event: EventWithMessage[YEvent]) -> None:
        q.put(event)

    consumer = Consumer(rmq_url, x.listeners)
    with run_consumer(consumer):
        y.publish(YEvent(name="y-service-event"))

        assert q.get(block=True, timeout=1) == "x_before_request"

        message: EventWithMessage = q.get()
        assert message.event == YEvent(name="y-service-event")

        assert q.get(block=True, timeout=1) == "x_after_request"


def test_service_middleware(
    make_service: Callable, run_consumer: Callable, rmq_url: str
) -> None:
    q: queue.Queue = queue.Queue()

    x: Service = make_service("X")
    y: Service = make_service("Y")

    @x.middleware
    def x_before_request(listener: Listener, message: PikaMessage) -> None:
        q.put("x_before_request")
        yield
        q.put("x_after_request")

    @x.on_event("Y", YEvent)
    def handle_y_event(event: EventWithMessage[YEvent]) -> None:
        q.put(event)

    consumer = Consumer(rmq_url, x.listeners)
    with run_consumer(consumer):
        y.publish(YEvent(name="y-service-event"))

        assert q.get(block=True, timeout=1) == "x_before_request"

        message: EventWithMessage = q.get()
        assert message.event == YEvent(name="y-service-event")

        assert q.get(block=True, timeout=1) == "x_after_request"


def test_service_middleware_exception(
    make_service: Callable, run_consumer: Callable, rmq_url: str
) -> None:
    q: queue.Queue = queue.Queue()

    x: Service = make_service("X")
    y: Service = make_service("Y")

    @x.middleware
    def x_before_request_1(listener: Listener, message: PikaMessage) -> None:
        q.put("x_before_request_1")
        try:
            yield
        finally:
            # This will be executed regardless of any exception in underlying middleware
            # (RuntimeException).
            q.put("x_after_request_1")

    @x.middleware
    def x_before_request_2(listener: Listener, message: PikaMessage) -> None:
        q.put("x_before_request_2")
        yield
        raise RuntimeError
        # This line will not be executed because of RuntimeError.
        q.put("x_after_request_2")

    @x.on_event("Y", YEvent)
    def handle_y_event(event: EventWithMessage[YEvent]) -> None:
        q.put(event)

    consumer = Consumer(rmq_url, x.listeners)
    with run_consumer(consumer):
        y.publish(YEvent(name="y-service-event"))

        assert q.get(block=True, timeout=1) == "x_before_request_1"
        assert q.get() == "x_before_request_2"

        message: EventWithMessage = q.get()
        assert message.event == YEvent(name="y-service-event")

        # x_after_request_2 should be there but because of exception
        # the code was not executed and next middleware layer gained control.

        assert q.get(block=True, timeout=1) == "x_after_request_1"

        with pytest.raises(queue.Empty):
            q.get(block=False)


def test_service_commands_reply_headers(
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
        y.send("X", Command(name="test"), reply_headers={"X-Saga-Id": 100})

        message: ReplyWithMessage = q.get(block=True, timeout=1)
        assert message.reply == "string-reply"
        assert message.message.properties.headers["X-Saga-Id"] == 100
