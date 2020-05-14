import queue
from dataclasses import dataclass
from typing import Callable

from myrabbit import EventWithMessage
from myrabbit.commands.command_with_message import CommandWithMessage
from myrabbit.core.consumer.consumer import Consumer
from myrabbit.service import Service


@dataclass
class YEvent:
    name: str


@dataclass
class Command:
    name: str


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
