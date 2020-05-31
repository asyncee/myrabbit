from collections import defaultdict
from typing import Callable, DefaultDict, List, Optional

from myrabbit.core.consumer.pika_message import PikaMessage

Callback = Callable


class Callbacks:
    def __init__(self, callbacks: Optional[DefaultDict[str, List[Callback]]] = None):
        self._callbacks = callbacks or defaultdict(list)

    def add_callback(self, name: str, callback: Callback) -> None:
        self._callbacks[name].append(callback)

    def before_request(self, message: PikaMessage) -> None:
        for cb in self._callbacks.get("before_request", []):
            cb(self, message)

    def after_request(self, message: PikaMessage) -> None:
        for cb in self._callbacks.get("after_request", []):
            cb(self, message)
