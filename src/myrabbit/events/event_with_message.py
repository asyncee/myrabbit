from dataclasses import dataclass
from typing import Generic, TypeVar

from myrabbit.core.consumer.pika_message import PikaMessage

EventType = TypeVar("EventType")


@dataclass
class EventWithMessage(Generic[EventType]):
    event: EventType
    message: PikaMessage
