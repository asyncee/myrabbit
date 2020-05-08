import abc
from typing import Tuple
from typing import Type

from pydantic import BaseModel

from myrabbit.events.event_with_message import T


class EventAdapter(abc.ABC):
    @abc.abstractmethod
    def name_and_body(self, event: T) -> Tuple[str, dict]:
        pass

    @abc.abstractmethod
    def name(self, event: Type[BaseModel]) -> str:
        pass

    @abc.abstractmethod
    def accepts(self, model: T) -> bool:
        pass

    def instantiate(self, event_type: Type[T], body: dict) -> T:
        return event_type(**body)
