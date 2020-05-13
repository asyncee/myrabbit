import json
from typing import Tuple

from .serializer import ContentType
from .serializer import Serializer


class JsonSerializer(Serializer):
    def serialize(self, data: dict) -> Tuple[ContentType, bytes]:
        return "application/json", json.dumps(data).encode("utf-8")

    def deserialize(self, data: bytes) -> dict:
        return json.loads(data.decode("utf-8"))
