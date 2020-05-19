from dataclasses import asdict
from dataclasses import is_dataclass
from typing import Tuple
from typing import Type

from .converter import Converter
from .converter import T


class DataclassConverter(Converter):
    def name_and_body(self, model: T) -> Tuple[str, dict]:
        return model.__class__.__name__, asdict(model)

    def name(self, model: Type[T]) -> str:
        return model.__name__

    def instantiate(self, model_class: Type[T], body: dict) -> T:
        return model_class(**body)  # type: ignore

    def accepts(self, model: T) -> bool:
        return is_dataclass(model)
