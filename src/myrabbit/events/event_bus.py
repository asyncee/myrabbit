import uuid
from typing import Callable
from typing import Optional

from pika import BasicProperties

from myrabbit.core.consumer.listener import Exchange
from myrabbit.core.consumer.listener import Listener
from myrabbit.core.consumer.listener import Queue
from myrabbit.core.consumer.pika_message import PikaMessage
from myrabbit.core.publisher.basic_publisher import BasicPublisher
from myrabbit.events.event_with_message import EventWithMessage
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
        self, event_source: str, event: str, body: Optional[dict] = None
    ) -> None:
        body = body or {}
        content_type, body = self._serializer.serialize(body)
        BasicPublisher(
            self._amqp_url, self._exchange(event_source), self._routing_key(event)
        ).publish(body, BasicProperties(content_type=content_type))

    def listener(
        self,
        event_source: str,
        event: str,
        callback: Callable[[EventWithMessage], None],
        exchange_params: dict = None,
        queue_params: dict = None,
    ) -> Listener:
        queue_params = queue_params or {}
        queue_params.setdefault("name", f"{callback.__name__}_{uuid.uuid4()}")
        queue_params = {**self.default_queue_params, **queue_params}
        exchange_params = exchange_params or {}
        exchange_params = {**self.default_exchange_params, **exchange_params}
        exchange_params.setdefault("type", "topic")
        exchange_params.setdefault("name", self._exchange(event_source))

        def deserialize_message(message: PikaMessage):
            callback(
                EventWithMessage(self._serializer.deserialize(message.body), message)
            )

        deserialize_message.__name__ = callback.__name__
        deserialize_message.__doc__ = callback.__doc__

        return Listener(
            exchange=Exchange(**exchange_params),
            queue=Queue(**queue_params),
            routing_key=event,
            handle_message=deserialize_message,
        )

    def _exchange(self, event_source: str) -> str:
        return f"{event_source}.events"

    def _routing_key(self, event: str) -> str:
        return event
