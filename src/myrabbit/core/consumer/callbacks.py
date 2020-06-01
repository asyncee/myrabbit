from collections import defaultdict
from contextlib import ExitStack, contextmanager
from typing import Callable, ContextManager, DefaultDict, List, Optional, Union

from myrabbit.core.consumer.pika_message import PikaMessage

Callback = Callable
Middleware = ContextManager


class Callbacks:
    def __init__(self, callbacks: Optional[DefaultDict[str, List[Callback]]] = None):
        self._callbacks = callbacks or defaultdict(list)

    def add_callback(self, name: str, callback: Union[Callback, Middleware]) -> None:
        self._callbacks[name].append(callback)

    def before_request(self, message: PikaMessage) -> None:
        for cb in self._callbacks.get("before_request", []):
            cb(self, message)

    def after_request(self, message: PikaMessage) -> None:
        for cb in self._callbacks.get("after_request", []):
            cb(self, message)

    def run_middleware(self, message: PikaMessage) -> ExitStack:
        with ExitStack() as stack:
            for middleware in self._callbacks.get("middleware", []):
                cm = contextmanager(middleware)
                stack.enter_context(cm(self, message))

            return stack.pop_all()
