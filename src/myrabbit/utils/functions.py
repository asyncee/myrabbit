import inspect
from functools import partial
from typing import Callable, Optional


class MethodNameError(Exception):
    pass


def get_method_name(method_name: Optional[str], fn: Callable) -> str:
    if isinstance(fn, partial):
        fn = fn.func

    if inspect.ismethod(fn.__call__):
        return method_name or fn.__class__.__name__

    if method_name:
        return method_name

    try:
        return fn.__name__
    except AttributeError:
        raise MethodNameError(
            f"Can not get method name for object {fn!r}. "
            f"Please, specify method name explicitly.  "
        )
