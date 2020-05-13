from .base import ListenEventStrategy


class ServicePool(ListenEventStrategy):
    def get_queue_name(
        self,
        event_destination: str,
        event_source: str,
        event_name: str,
        method_name: str,
    ) -> str:
        return f"{event_source}.{event_name}.to.{event_destination}.{method_name}"
