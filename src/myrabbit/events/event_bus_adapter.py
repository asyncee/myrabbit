from functools import wraps
from typing import Callable
from typing import List
from typing import Optional
from typing import Type

from pika import BasicProperties

from myrabbit.core.consumer.listener import Listener
from myrabbit.core.converter import Converter
from myrabbit.core.converter import DEFAULT_CONVERTERS
from myrabbit.events.event_bus import EventBus
from myrabbit.events.event_with_message import EventType
from myrabbit.events.event_with_message import EventWithMessage
from myrabbit.events.listen_event_strategy import ListenEventStrategy


class EventBusAdapter:
    def __init__(
        self, event_bus: EventBus, converters: Optional[List[Converter]] = None,
    ):
        self.event_bus = event_bus
        self.converters = converters or DEFAULT_CONVERTERS

    def _converter(self, event: EventType) -> Converter:
        for converter in self.converters:
            if converter.accepts(event):
                return converter
        raise RuntimeError("Converter not found")

    def publish(
        self,
        event_source: str,
        event: EventType,
        properties: Optional[BasicProperties] = None,
    ) -> None:
        event_name, body = self._converter(event).name_and_body(event)
        self.event_bus.publish(event_source, event_name, body, properties)

    def listener(
        self,
        event_destination: str,
        event_source: str,
        event_type: Type[EventType],
        callback: Callable[[EventWithMessage], None],
        exchange_params: Optional[dict] = None,
        queue_params: Optional[dict] = None,
        listen_strategy: Optional[ListenEventStrategy] = None,
        method_name: Optional[str] = None,
    ) -> Listener:
        converter = self._converter(event_type)

        @wraps(callback)
        def instantiate_event(event_with_message: EventWithMessage) -> None:
            event_with_message.event = converter.instantiate(
                event_type, event_with_message.event
            )
            callback(event_with_message)

        event_name = converter.name(event_type)

        return self.event_bus.listener(
            event_destination=event_destination,
            event_source=event_source,
            event_name=event_name,
            callback=instantiate_event,
            exchange_params=exchange_params,
            queue_params=queue_params,
            listen_strategy=listen_strategy,
            method_name=method_name,
        )
