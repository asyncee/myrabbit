from functools import wraps
from typing import Callable
from typing import List
from typing import Optional
from typing import Type

from myrabbit.core.consumer.listener import Listener
from myrabbit.events.event_adapter import DataclassEventAdapter
from myrabbit.events.event_adapter import EventAdapter
from myrabbit.events.event_adapter import PydanticEventAdapter
from myrabbit.events.event_bus import EventBus
from myrabbit.events.event_with_message import EventWithMessage
from myrabbit.events.event_with_message import T
from myrabbit.events.listen_event_strategy import ListenEventStrategy


class EventBusAdapter:
    DEFAULT_EVENT_ADAPTERS = [DataclassEventAdapter(), PydanticEventAdapter()]

    def __init__(
        self,
        base_event_bus: EventBus,
        event_adapters: Optional[List[EventAdapter]] = None,
    ):
        self.impl = base_event_bus
        self.event_adapters = event_adapters or self.DEFAULT_EVENT_ADAPTERS

    def _adapter(self, event: T) -> EventAdapter:
        for adapter in self.event_adapters:
            if adapter.accepts(event):
                return adapter
        raise RuntimeError("Adapter not found")

    def publish(self, event_source: str, event_name: T) -> None:
        event_name, body = self._adapter(event_name).name_and_body(event_name)
        return self.impl.publish(event_source, event_name, body)

    def listener(
        self,
        event_destination: str,
        event_source: str,
        event_type: Type[T],
        callback: Callable[[EventWithMessage], None],
        exchange_params: dict = None,
        queue_params: dict = None,
        listen_strategy: Optional[ListenEventStrategy] = None,
        method_name: Optional[str] = None,
    ) -> Listener:
        adapter = self._adapter(event_type)

        @wraps(callback)
        def instantiate_event(event_with_message: EventWithMessage):
            event_with_message.event = adapter.instantiate(
                event_type, event_with_message.event
            )
            callback(event_with_message)

        event_name = adapter.name(event_type)

        return self.impl.listener(
            event_destination=event_destination,
            event_source=event_source,
            event_name=event_name,
            callback=instantiate_event,
            exchange_params=exchange_params,
            queue_params=queue_params,
            listen_strategy=listen_strategy,
            method_name=method_name,
        )
