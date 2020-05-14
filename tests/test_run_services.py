from dataclasses import dataclass
from typing import Callable
from unittest import mock

from myrabbit import EventWithMessage
from myrabbit.runner import run_services
from myrabbit.service import Service


@dataclass
class TestEvent:
    pass


def test_run_services(rmq_url: str, make_service: Callable):
    a: Service = make_service("A")
    b: Service = make_service("B")

    @a.on_event("B", TestEvent)
    def handle_b(event: EventWithMessage) -> None:
        # stub
        pass

    @b.on_event("A", TestEvent)
    def handle_a(event: EventWithMessage) -> None:
        # stub
        pass

    with mock.patch("myrabbit.runner.Consumer") as m:
        run_services(rmq_url, a, b)
        assert m.called_with(rmq_url, a.listeners + b.listeners)
        assert m.run.called_once()
