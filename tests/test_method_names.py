from typing import Any

from myrabbit import EventBus
from myrabbit.utils.functions import get_method_name


def test_get_method_name() -> None:
    def inner_func() -> None:  # noqa
        pass

    class TestCallback:
        def __call__(self) -> None:  # noqa
            pass

    assert get_method_name(None, inner_func) == 'inner_func'
    assert get_method_name(None, TestCallback()) == 'TestCallback'
    assert get_method_name('one', inner_func) == 'one'
    assert get_method_name('two', TestCallback()) == 'two'


def test_queue_name(event_bus: EventBus) -> None:
    def a_callback(evt: Any) -> None:  # noqa
        pass

    class TestCallback:
        def __call__(self) -> None:  # noqa
            pass

    listener = event_bus.listener("dest", "src", "event", a_callback)
    assert listener.queue.name == "src.event.to.dest.a_callback"
    listener = event_bus.listener("dest", "src", "event", TestCallback())
    assert listener.queue.name == "src.event.to.dest.TestCallback"
