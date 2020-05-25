import inspect
from typing import Callable
from typing import Optional


def get_method_name(method_name: Optional[str], fn: Callable) -> str:
    if inspect.ismethod(fn.__call__):
        return method_name or fn.__class__.__name__
    return method_name or fn.__name__
