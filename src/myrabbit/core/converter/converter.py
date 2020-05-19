import abc
from typing import Tuple
from typing import Type
from typing import TypeVar

T = TypeVar("T")


class Converter(abc.ABC):
    @abc.abstractmethod
    def name_and_body(self, model: T) -> Tuple[str, dict]:
        pass

    @abc.abstractmethod
    def name(self, model: Type[T]) -> str:
        pass

    @abc.abstractmethod
    def accepts(self, model: T) -> bool:
        pass

    def instantiate(self, model_class: Type[T], body: dict) -> T:
        return model_class(**body)  # type: ignore
