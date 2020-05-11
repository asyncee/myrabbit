from dataclasses import asdict
from dataclasses import is_dataclass
from typing import Tuple
from typing import Type

from pydantic import BaseModel

from myrabbit.events.event_with_message import EventType
from .event_adapter import EventAdapter


class DataclassEventAdapter(EventAdapter):
    def name_and_body(self, event) -> Tuple[str, dict]:
        return event.__class__.__name__, asdict(event)

    def name(self, event: Type[BaseModel]) -> str:
        return event.__name__

    def instantiate(self, event_type: Type[EventType], body: dict) -> EventType:
        return event_type(**body)

    def accepts(self, model: EventType) -> bool:
        return is_dataclass(model)
