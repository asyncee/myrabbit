from typing import Callable, Optional, Type

from myrabbit import CommandBus, EventBus, Service
from myrabbit.commands.command_with_message import (
    CommandReplyType,
    CommandType,
    ReplyWithMessage,
)
from myrabbit.core.consumer.callbacks import Callback, Middleware
from myrabbit.events.event_with_message import EventType
from myrabbit.events.listen_event_strategy import ListenEventStrategy


class ServiceBuilder:
    def __init__(self, service_name: str):
        self.service_name = service_name
        self._calls = []

    def before_request(self, fn: Callback) -> Callback:
        self._calls.append(lambda s: s.before_request(fn))
        return fn

    def after_request(self, fn: Callback) -> Callback:
        self._calls.append(lambda s: s.after_request(fn))
        return fn

    def middleware(self, fn: Middleware) -> Middleware:
        self._calls.append(lambda s: s.middleware(fn))
        return fn

    def on_event(
        self,
        event_source: str,
        event_type: Type[EventType],
        exchange_params: Optional[dict] = None,
        queue_params: Optional[dict] = None,
        listen_strategy: Optional[ListenEventStrategy] = None,
        method_name: Optional[str] = None,
    ) -> Callable:
        def register_event_listener(fn):
            self._calls.append(
                lambda s: s.on_event(
                    event_source,
                    event_type,
                    exchange_params,
                    queue_params,
                    listen_strategy,
                    method_name,
                )(fn)
            )
            return fn

        return register_event_listener

    def on_command(
        self,
        command_type: Type[CommandType],
        exchange_params: Optional[dict] = None,
        queue_params: Optional[dict] = None,
    ) -> Callable:
        def register_command_listener(fn: Callable) -> Callable:
            self._calls.append(
                lambda s: s.on_command(command_type, exchange_params, queue_params)(fn)
            )
            return fn

        return register_command_listener

    def on_command_reply(
        self,
        command_destination: str,
        command_type: Type[CommandType],
        reply_type: Optional[Type[CommandReplyType]] = None,
        listen_on: Optional[str] = None,
        exchange_params: Optional[dict] = None,
        queue_params: Optional[dict] = None,
    ) -> Callable:
        def register_command_listener(
            fn: Callable[[ReplyWithMessage], None]
        ) -> Callable:
            self._calls.append(
                lambda s: s.on_command_reply(
                    command_destination,
                    command_type,
                    reply_type,
                    listen_on,
                    exchange_params,
                    queue_params,
                )(fn)
            )
            return fn

        return register_command_listener

    def build(self, event_bus: EventBus, command_bus: CommandBus):
        s = Service(self.service_name, event_bus, command_bus)
        for call in self._calls:
            call(s)
        return s
