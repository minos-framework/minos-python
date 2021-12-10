from __future__ import (
    annotations,
)

from asyncio import (
    iscoroutinefunction,
)
from collections.abc import (
    Iterable,
)
from datetime import (
    timedelta,
)
from typing import (
    TYPE_CHECKING,
    Optional,
    Union,
)

from ..callables import (
    Checker,
    CheckerMeta,
    CheckerWrapper,
)

if TYPE_CHECKING:
    from .abc import (
        Handler,
    )


class CheckDecorator:
    """Enroute Check Decorator class."""

    def __init__(
        self,
        max_attempts: int = 10,
        delay: Union[float, timedelta] = 0.1,
        _checkers: Optional[set[CheckerMeta]] = None,
        _handler: Optional[Handler] = None,
    ):
        if isinstance(delay, timedelta):
            delay = delay.total_seconds()

        self.max_attempts = max_attempts
        self.delay = delay

        self._checkers = _checkers
        self._handler = _handler

    def __call__(self, func: Union[CheckerWrapper, Checker]) -> CheckerWrapper:
        if isinstance(func, CheckerWrapper):
            func = func.meta.func

        if iscoroutinefunction(func) and not iscoroutinefunction(self._handler):
            raise ValueError(f"{self._handler!r} must be a coroutine if {func!r} is a coroutine")

        meta = CheckerMeta(func, self.max_attempts, self.delay)

        if self._checkers is not None:
            self._checkers.add(meta)

        return meta.wrapper

    def __repr__(self):
        args = ", ".join(map(repr, self))
        return f"{type(self).__name__}({args})"

    def __eq__(self, other: CheckDecorator) -> bool:
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> Iterable:
        yield from (
            self.max_attempts,
            self.delay,
        )
