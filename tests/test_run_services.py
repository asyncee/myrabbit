from dataclasses import dataclass
from unittest import mock

from myrabbit import EventBus
from myrabbit.run_services import run_services
from myrabbit.service import Service


@dataclass
class TestEvent:
    pass


def test_run_services(rmq_url, event_bus: EventBus):
    a = Service("A", event_bus)
    b = Service("B", event_bus)

    @a.on("B", TestEvent)
    def handle_b(*args, **kwargs):
        pass

    @b.on("A", TestEvent)
    def handle_a(*args, **kwargs):
        pass

    with mock.patch("myrabbit.run_services.Consumer") as m:
        run_services(rmq_url, a, b)
        assert m.called_with(rmq_url, a.listeners + b.listeners)
        assert m.run.called_once()
