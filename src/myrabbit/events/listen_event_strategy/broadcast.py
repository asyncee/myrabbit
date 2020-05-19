import uuid
from typing import Optional

from .base import ListenEventStrategy


class Broadcast(ListenEventStrategy):
    def __init__(self, broadcast_identifier: Optional[str] = None):
        self._broadcast_identifier = broadcast_identifier or uuid.uuid4().hex

    def get_queue_name(
        self,
        event_destination: str,
        event_source: str,
        event_name: str,
        method_name: str,
    ) -> str:
        return (
            f"{event_source}.{event_name}.to."
            f"{event_destination}.{method_name}:{self._broadcast_identifier}"
        )
