from typing import Tuple
from typing import Type

from pydantic import BaseModel

from .converter import Converter
from .converter import T


class PydanticConverter(Converter):
    def name_and_body(self, model: T) -> Tuple[str, dict]:
        return model.__class__.__name__, model.dict()  # type: ignore

    def name(self, model: Type[T]) -> str:
        return model.__name__

    def instantiate(self, model_class: Type[T], body: dict) -> T:
        return model_class(**body)  # type: ignore

    def accepts(self, model: T) -> bool:
        try:
            return isinstance(model, BaseModel) or issubclass(model, BaseModel)  # type: ignore
        except TypeError:
            return False
