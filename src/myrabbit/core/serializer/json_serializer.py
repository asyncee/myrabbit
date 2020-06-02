from typing import Any, Tuple

import orjson

from .serializer import ContentType, Serializer


class JsonSerializer(Serializer):
    def serialize(self, data: Any) -> Tuple[ContentType, bytes]:
        return "application/json", orjson.dumps(data)

    def deserialize(self, data: bytes) -> Any:
        return orjson.loads(data.decode("utf-8"))
