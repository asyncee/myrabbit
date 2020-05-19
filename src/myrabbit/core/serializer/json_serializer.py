from typing import Any
from typing import Tuple

import orjson

from .serializer import ContentType
from .serializer import Serializer


class JsonSerializer(Serializer):
    def serialize(self, data: Any) -> Tuple[ContentType, bytes]:
        return "application/json", orjson.dumps(data)

    def deserialize(self, data: bytes) -> Any:
        return orjson.loads(data.decode("utf-8"))
