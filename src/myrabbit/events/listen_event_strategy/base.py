import abc


class ListenEventStrategy(abc.ABC):
    """Strategy that setups event routing.

    The idea and documentation are taken from beautiful Nameko framework.

    https://github.com/nameko/nameko/blob/v3.0.0-rc/nameko/events.py
    """

    @abc.abstractmethod
    def get_queue_name(
        self,
        event_destination: str,
        event_source: str,
        event_name: str,
        method_name: str,
    ) -> str:
        pass
