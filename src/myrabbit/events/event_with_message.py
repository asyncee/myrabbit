from dataclasses import dataclass
from typing import Generic
from typing import TypeVar

from myrabbit.core.consumer.pika_message import PikaMessage

T = TypeVar("T")


@dataclass
class EventWithMessage(Generic[T]):
    event: T
    message: PikaMessage
