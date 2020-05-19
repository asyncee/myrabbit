import abc
from typing import Any
from typing import Tuple


ContentType = str


class Serializer(abc.ABC):
    @abc.abstractmethod
    def serialize(self, data: Any) -> Tuple[ContentType, bytes]:
        pass

    @abc.abstractmethod
    def deserialize(self, data: bytes) -> Any:
        pass
