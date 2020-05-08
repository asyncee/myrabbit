from dataclasses import asdict
from dataclasses import is_dataclass
from typing import Tuple
from typing import Type

from pydantic import BaseModel

from myrabbit.events.event_adapter import EventAdapter
from myrabbit.events.event_with_message import T


class DataclassEventAdapter(EventAdapter):
    def name_and_body(self, event) -> Tuple[str, dict]:
        return event.__class__.__name__, asdict(event)

    def name(self, event: Type[BaseModel]) -> str:
        return event.__name__

    def instantiate(self, event_type: Type[T], body: dict) -> T:
        return event_type(**body)

    def accepts(self, model: T) -> bool:
        return is_dataclass(model)
