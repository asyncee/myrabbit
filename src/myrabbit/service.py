from typing import Callable
from typing import List
from typing import Optional
from typing import Type

from pika import BasicProperties

from myrabbit import EventBus
from myrabbit import EventBusAdapter
from myrabbit.commands.command_bus import CommandBus
from myrabbit.commands.command_bus_adapter import CommandBusAdapter
from myrabbit.commands.command_with_message import CommandReplyType
from myrabbit.commands.command_with_message import CommandType
from myrabbit.commands.command_with_message import ReplyWithMessage
from myrabbit.core.consumer.callbacks import Callback
from myrabbit.core.consumer.callbacks import Callbacks
from myrabbit.core.consumer.callbacks import Middleware
from myrabbit.core.consumer.listener import Listener
from myrabbit.events.event_with_message import EventType
from myrabbit.events.listen_event_strategy import ListenEventStrategy


class Service:
    def __init__(
        self, service_name: str, event_bus: EventBus, command_bus: CommandBus,
    ):
        self.service_name = service_name
        self._event_bus = event_bus
        self._event_bus_adapter = self._make_event_bus_adapter(self._event_bus)
        self._command_bus = command_bus
        self._command_bus_adapter = self._make_command_bus_adapter(self._command_bus)

        self._callbacks = Callbacks()
        # Override callbacks for event bus and command bus.
        # Because callbacks is mutable, instantiated listeners will
        # receive newly added callbacks.
        # This is ugly solution and right approach is to use Composite
        # pattern for `Callbacks` or Builder for `Service`.
        self._event_bus.set_callbacks(self._callbacks)
        self._command_bus.set_callbacks(self._callbacks)
        self._listeners: List[Listener] = []

    def before_request(self, fn: Callback) -> Callback:
        self._callbacks.add_callback("before_request", fn)
        return fn

    def after_request(self, fn: Callback) -> Callback:
        self._callbacks.add_callback("after_request", fn)
        return fn

    def middleware(self, fn: Middleware) -> Middleware:
        self._callbacks.add_callback("middleware", fn)
        return fn

    @property
    def event_bus(self) -> EventBus:
        return self._event_bus

    @property
    def command_bus(self) -> CommandBus:
        return self._command_bus

    @property
    def listeners(self) -> List[Listener]:
        return self._listeners

    def publish(
        self, event: EventType, properties: Optional[BasicProperties] = None
    ) -> None:
        properties = properties or BasicProperties()
        properties.app_id = self.service_name
        self._event_bus_adapter.publish(
            event_source=self.service_name, event=event, properties=properties
        )

    def send(
        self,
        command_destination: str,
        command: CommandType,
        properties: Optional[BasicProperties] = None,
        reply_to: Optional[str] = None,
    ) -> None:
        properties = properties or BasicProperties()
        properties.app_id = self.service_name

        if reply_to is not None:
            properties.reply_to = reply_to

        self._command_bus_adapter.send(
            self.service_name,
            command_destination=command_destination,
            command=command,
            properties=properties,
        )

    def on_event(
        self,
        event_source: str,
        event_type: Type[EventType],
        exchange_params: Optional[dict] = None,
        queue_params: Optional[dict] = None,
        listen_strategy: Optional[ListenEventStrategy] = None,
        method_name: Optional[str] = None,
    ) -> Callable:
        def register_event_listener(fn: Callable) -> Callable:
            self._listeners.append(
                self._event_bus_adapter.listener(
                    event_destination=self.service_name,
                    event_source=event_source,
                    event_type=event_type,
                    callback=fn,
                    exchange_params=exchange_params,
                    queue_params=queue_params,
                    listen_strategy=listen_strategy,
                    method_name=method_name,
                )
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
            self._listeners.append(
                self._command_bus_adapter.listener(
                    command_destination=self.service_name,
                    command_type=command_type,
                    callback=fn,
                    exchange_params=exchange_params,
                    queue_params=queue_params,
                )
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
        if listen_on:
            queue_params = queue_params or {}
            queue_params["name"] = listen_on

        def register_command_listener(
            fn: Callable[[ReplyWithMessage], None]
        ) -> Callable:
            self._listeners.append(
                self._command_bus_adapter.reply_listener(
                    command_sender=self.service_name,
                    command_destination=command_destination,
                    command_type=command_type,
                    callback=fn,
                    reply_type=reply_type,
                    exchange_params=exchange_params,
                    queue_params=queue_params,
                )
            )
            return fn

        return register_command_listener

    def _make_event_bus_adapter(self, event_bus: EventBus) -> EventBusAdapter:
        return EventBusAdapter(event_bus)

    def _make_command_bus_adapter(self, command_bus: CommandBus) -> CommandBusAdapter:
        return CommandBusAdapter(command_bus)
