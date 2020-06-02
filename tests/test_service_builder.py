from dataclasses import dataclass

from myrabbit import Service, ServiceBuilder


def get_wrapped(fn):
    while getattr(fn, "__wrapped__", None) is not None:
        fn = fn.__wrapped__
    return fn


def test_empty_service_builder(event_bus, command_bus):
    builder = ServiceBuilder("test")

    result = builder.build(event_bus, command_bus)
    expected = Service("test", event_bus, command_bus)

    assert result.service_name == expected.service_name
    assert result.service_name == expected.service_name
    assert result._event_bus == expected._event_bus
    assert result._command_bus == expected._command_bus
    assert result._callbacks == expected._callbacks
    assert result._listeners == expected._listeners


def test_complex_service_builder(event_bus, command_bus):
    builder = ServiceBuilder("test")

    before = lambda: None
    after = lambda: None
    middleware = lambda: None
    on_command = lambda: None
    on_event = lambda: None
    on_command_reply = lambda: None

    @dataclass
    class Cmd:
        pass

    builder.before_request(before)
    builder.after_request(after)
    builder.middleware(middleware)
    builder.on_command(Cmd)(on_command)
    builder.on_event("event-source", Cmd)(on_event)
    builder.on_command_reply("command-destination", Cmd)(on_command_reply)

    expected = Service("test", event_bus, command_bus)
    expected.before_request(before)
    expected.after_request(after)
    expected.middleware(middleware)
    expected.on_command(Cmd)(on_command)
    expected.on_event("event-source", Cmd)(on_event)
    expected.on_command_reply("command-destination", Cmd)(on_command_reply)

    result = builder.build(event_bus, command_bus)
    assert result.service_name == expected.service_name
    assert result.service_name == expected.service_name
    assert result._event_bus == expected._event_bus
    assert result._command_bus == expected._command_bus
    assert result._callbacks._callbacks == expected._callbacks._callbacks
    assert result._callbacks == expected._callbacks

    for r, e in zip(result._listeners, expected._listeners):
        assert r.exchange == e.exchange
        assert r.queue == e.queue
        assert r.routing_key == e.routing_key
        assert r.auto_ack == e.auto_ack
        assert r.handle_message_strategy == e.handle_message_strategy
        assert r.callbacks == e.callbacks
        assert get_wrapped(r.handle_message) == get_wrapped(e.handle_message)
