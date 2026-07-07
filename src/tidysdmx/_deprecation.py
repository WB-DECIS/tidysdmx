"""Internal helper for marking public functions as deprecated.

Provides the :func:`deprecated` decorator used across tidysdmx to emit a
uniform ``FutureWarning`` when a deprecated public function is called. We use
``FutureWarning`` (shown to end users by default) rather than
``DeprecationWarning`` (hidden outside ``__main__`` and test runners) so that
library consumers actually see the notice and can migrate.
"""

import functools
import warnings
from collections.abc import Callable
from typing import TypeVar

F = TypeVar("F", bound=Callable[..., object])


def deprecated(
    *,
    replacement: str | None = None,
    removal: str = "a future release",
) -> Callable[[F], F]:
    """Return a decorator that flags a public function as deprecated.

    The decorated function keeps its original behaviour but emits a
    ``FutureWarning`` on every call and gains a ``__deprecated__`` attribute
    (mirroring :pep:`702`). Apply it **above** ``@typechecked`` so the warning
    fires before the type-checked call runs; ``functools.wraps`` preserves the
    wrapped function's signature, annotations, and docstring.

    Args:
        replacement: What callers should use instead. When ``None``, the
            message omits a suggested replacement (for functions being retired
            with no successor).
        removal: Human-readable description of when the function will be
            removed. Defaults to ``"a future release"``.

    Returns:
        A decorator that wraps the target function.
    """

    def decorator(func: F) -> F:
        message = f"{func.__name__} is deprecated and will be removed in {removal}."
        if replacement is not None:
            message += f" Use {replacement} instead."

        @functools.wraps(func)
        def wrapper(*args: object, **kwargs: object) -> object:
            warnings.warn(message, FutureWarning, stacklevel=2)
            return func(*args, **kwargs)

        wrapper.__deprecated__ = message
        return wrapper  # type: ignore[return-value]

    return decorator
