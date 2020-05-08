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
from myrabbit.events.serializer import Serializer
from myrabbit.events.serializer.json_serializer import JsonSerializer


class EventBus:
    def __init__(
        self, amqp_url: str, serializer: Optional[Serializer] = None,
    ):
        self._amqp_url = amqp_url
        self._serializer: Serializer = serializer or JsonSerializer()

    def publish(self, event_source: str, event: str, body: dict) -> None:
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
        queue_name = f"{callback.__name__}_{uuid.uuid4()}"
        queue_params = queue_params or {}
        exchange_params = exchange_params or {}
        exchange_params.setdefault("type", "topic")

        def deserialize_message(message: PikaMessage):
            callback(
                EventWithMessage(self._serializer.deserialize(message.body), message)
            )

        deserialize_message.__name__ = callback.__name__
        deserialize_message.__doc__ = callback.__doc__

        return Listener(
            exchange=Exchange(name=self._exchange(event_source), **exchange_params),
            queue=Queue(name=queue_name, **queue_params),
            routing_key=event,
            handle_message=deserialize_message,
        )

    def _exchange(self, event_source: str) -> str:
        return f"{event_source}.events"

    def _routing_key(self, event: str) -> str:
        return event
