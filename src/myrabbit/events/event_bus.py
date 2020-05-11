from functools import wraps
from typing import Callable
from typing import Optional

from pika import BasicProperties

from myrabbit.core.consumer.listener import Exchange
from myrabbit.core.consumer.listener import Listener
from myrabbit.core.consumer.listener import Queue
from myrabbit.core.consumer.pika_message import PikaMessage
from myrabbit.core.publisher.basic_publisher import BasicPublisher
from myrabbit.events.event_with_message import EventWithMessage
from myrabbit.events.listen_event_strategy.base import ListenEventStrategy
from myrabbit.events.listen_event_strategy.service_pool import ServicePool
from myrabbit.events.serializer import JsonSerializer
from myrabbit.events.serializer import Serializer


class EventBus:
    def __init__(
        self,
        amqp_url: str,
        serializer: Optional[Serializer] = None,
        default_exchange_params: Optional[dict] = None,
        default_queue_params: Optional[dict] = None,
    ):
        self._amqp_url = amqp_url
        self._serializer: Serializer = serializer or JsonSerializer()
        self.default_exchange_params = default_exchange_params or {}
        self.default_queue_params = default_queue_params or {}

    def publish(
        self,
        event_source: str,
        event: str,
        body: Optional[dict] = None,
        properties: Optional[BasicProperties] = None,
    ) -> None:
        properties = properties or BasicProperties()

        body = body or {}
        content_type, body = self._serializer.serialize(body)
        properties.content_type = content_type
        publisher = self._make_publisher(event_source=event_source, event_name=event)
        publisher.publish(body, properties)

    def _make_publisher(self, event_source: str, event_name: str) -> BasicPublisher:
        return BasicPublisher(
            self._amqp_url, self._exchange(event_source), self._routing_key(event_name)
        )

    def listener(
        self,
        event_destination: str,
        event_source: str,
        event_name: str,
        callback: Callable[[EventWithMessage], None],
        exchange_params: dict = None,
        queue_params: dict = None,
        listen_strategy: Optional[ListenEventStrategy] = None,
        method_name: Optional[str] = None,
    ) -> Listener:
        listen_strategy = listen_strategy or ServicePool()

        method_name = method_name or getattr(callback, __name__, None) or repr(callback)

        queue_params = queue_params or {}
        queue_params = {**self.default_queue_params, **queue_params}
        queue_params.setdefault(
            "name",
            listen_strategy.get_queue_name(
                event_destination, event_source, event_name, method_name,
            ),
        )

        exchange_params = exchange_params or {}
        exchange_params = {**self.default_exchange_params, **exchange_params}
        exchange_params.setdefault("type", "topic")
        exchange_params.setdefault("name", self._exchange(event_source))

        @wraps(callback)
        def deserialize_message(message: PikaMessage):
            callback(
                EventWithMessage(self._serializer.deserialize(message.body), message)
            )

        return Listener(
            exchange=Exchange(**exchange_params),
            queue=Queue(**queue_params),
            routing_key=event_name,
            handle_message=deserialize_message,
        )

    def _exchange(self, event_source: str) -> str:
        return f"{event_source}.events"

    def _routing_key(self, event: str) -> str:
        return event
