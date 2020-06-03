from typing import Callable, List, Optional, Type

from pika import BasicProperties

from myrabbit import EventBus, EventBusAdapter
from myrabbit.commands.command_bus import CommandBus
from myrabbit.commands.command_bus_adapter import CommandBusAdapter
from myrabbit.commands.command_with_message import CommandReplyType, CommandType, ReplyWithMessage
from myrabbit.core.consumer.callbacks import Callback, Callbacks, Middleware
from myrabbit.core.consumer.listener import Listener
from myrabbit.events.event_with_message import EventType
from myrabbit.events.listen_event_strategy import ListenEventStrategy
from myrabbit.service.doc import Doc


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
        self.doc = Doc()

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
        reply_headers: Optional[dict] = None,
    ) -> None:
        properties = properties or BasicProperties()
        properties.app_id = self.service_name

        if reply_to is not None:
            converter = self._command_bus_adapter.get_converter(command)
            command_name = converter.name(command.__class__)
            properties.reply_to = self._reply_queue_name(
                command_destination, command_name, reply_to
            )

        self._command_bus_adapter.send(
            self.service_name,
            command_destination=command_destination,
            command=command,
            properties=properties,
            reply_headers=reply_headers,
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
        self.doc.add_event(event_source, event_type)

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
        self.doc.add_command(command_type)

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
        self.doc.add_command_reply(command_destination, command_type)

        if listen_on:
            queue_params = queue_params or {}
            converter = self._command_bus_adapter.get_converter(command_type)
            command_name = converter.name(command_type)
            queue_params["name"] = self._reply_queue_name(
                command_destination, command_name, listen_on
            )

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

    def _reply_queue_name(
        self, command_destination: str, command_name: str, queue_name: str
    ) -> str:
        default_name = self._command_bus.reply_queue_name(
            self.service_name, command_destination, command_name
        )
        return f"{default_name}:{queue_name}"
