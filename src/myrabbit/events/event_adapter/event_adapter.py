import abc
from typing import Tuple
from typing import Type

from pydantic import BaseModel

from myrabbit.events.event_with_message import EventType


class EventAdapter(abc.ABC):
    @abc.abstractmethod
    def name_and_body(self, event: EventType) -> Tuple[str, dict]:
        pass

    @abc.abstractmethod
    def name(self, event: Type[BaseModel]) -> str:
        pass

    @abc.abstractmethod
    def accepts(self, model: EventType) -> bool:
        pass

    def instantiate(self, event_type: Type[EventType], body: dict) -> EventType:
        return event_type(**body)
