from typing import List
from typing import Optional
from typing import Type

from pika import BasicProperties

from myrabbit import EventBus
from myrabbit import EventBusAdapter
from myrabbit.core.consumer.listener import Listener
from myrabbit.events.event_with_message import EventType
from myrabbit.events.listen_event_strategy import ListenEventStrategy


class Service:
    def __init__(self, service_name: str, event_bus: EventBus):
        self.service_name = service_name
        self._event_bus = event_bus
        self._event_bus_adapter = self._make_adapter(event_bus)

        self._listeners: List[Listener] = []

    @property
    def event_bus(self) -> EventBus:
        return self._event_bus

    @property
    def listeners(self) -> List[Listener]:
        return self._listeners

    def publish(self, event: EventType, properties: Optional[BasicProperties] = None):
        properties = properties or BasicProperties()
        properties.app_id = self.service_name
        self._event_bus_adapter.publish(
            event_source=self.service_name, event=event, properties=properties
        )

    def on(
        self,
        event_source: str,
        event_type: Type[EventType],
        exchange_params: dict = None,
        queue_params: dict = None,
        listen_strategy: Optional[ListenEventStrategy] = None,
        method_name: Optional[str] = None,
    ):
        def callback_wrapper(fn):
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

        return callback_wrapper

    def _make_adapter(self, event_bus: EventBus):
        return EventBusAdapter(event_bus)
