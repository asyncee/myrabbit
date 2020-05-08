import abc
from typing import Tuple


ContentType = str


class Serializer(abc.ABC):
    @abc.abstractmethod
    def serialize(self, data: dict) -> Tuple[ContentType, bytes]:
        pass

    @abc.abstractmethod
    def deserialize(self, data: bytes) -> dict:
        pass
