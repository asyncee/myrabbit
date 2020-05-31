from functools import wraps
from typing import Callable, Optional

from pika import BasicProperties

from myrabbit.core.consumer.callbacks import Callbacks
from myrabbit.core.consumer.listener import Exchange, Listener, Queue
from myrabbit.core.consumer.pika_message import PikaMessage
from myrabbit.core.publisher.reconnecting_publisher import \
    ReconnectingPublisherFactory
from myrabbit.core.serializer import JsonSerializer, Serializer
from myrabbit.events.event_with_message import EventWithMessage
from myrabbit.events.listen_event_strategy.base import ListenEventStrategy
from myrabbit.events.listen_event_strategy.service_pool import ServicePool
from myrabbit.utils.functions import get_method_name


class EventBus:
    def __init__(
        self,
        amqp_url: str,
        serializer: Optional[Serializer] = None,
        default_exchange_params: Optional[dict] = None,
        default_queue_params: Optional[dict] = None,
        callbacks: Optional[Callbacks] = None,
    ):
        self._amqp_url = amqp_url
        self._serializer: Serializer = serializer or JsonSerializer()
        self.default_exchange_params = default_exchange_params or {}
        self.default_queue_params = default_queue_params or {}
        self._publisher_factory = ReconnectingPublisherFactory(self._amqp_url)
        self._callbacks = callbacks

    def set_callbacks(self, callbacks: Callbacks) -> None:
        self._callbacks = callbacks

    def publish(
        self,
        event_source: str,
        event_name: str,
        body: Optional[dict] = None,
        properties: Optional[BasicProperties] = None,
    ) -> None:
        properties = properties or BasicProperties()

        body = body or {}
        content_type, binary_body = self._serializer.serialize(body)
        properties.content_type = content_type

        with self._publisher_factory.publisher() as publisher:
            publisher.publish(
                self._exchange(event_source),
                self._routing_key(event_name),
                binary_body,
                properties,
            )

    def listener(
        self,
        event_destination: str,
        event_source: str,
        event_name: str,
        callback: Callable[[EventWithMessage], None],
        exchange_params: Optional[dict] = None,
        queue_params: Optional[dict] = None,
        listen_strategy: Optional[ListenEventStrategy] = None,
        method_name: Optional[str] = None,
    ) -> Listener:
        listen_strategy = listen_strategy or ServicePool()

        method_name = get_method_name(method_name, callback)

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
        def deserialize_message(message: PikaMessage) -> None:
            callback(
                EventWithMessage(self._serializer.deserialize(message.body), message)
            )

        return Listener(
            exchange=Exchange(**exchange_params),
            queue=Queue(**queue_params),
            routing_key=event_name,
            handle_message=deserialize_message,
            callbacks=self._callbacks,
        )

    def _exchange(self, event_source: str) -> str:
        return f"{event_source}.events"

    def _routing_key(self, event: str) -> str:
        return event
