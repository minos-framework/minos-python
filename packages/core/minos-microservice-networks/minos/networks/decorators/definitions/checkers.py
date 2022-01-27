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
    Union,
)

from ..callables import (
    Checker,
    CheckerMeta,
    CheckerWrapper,
    Handler,
    HandlerMeta,
)


class CheckDecorator:
    """Enroute Check Decorator class."""

    def __init__(self, handler_meta: HandlerMeta, max_attempts: int = 10, delay: Union[float, timedelta] = 0.1):
        if isinstance(delay, timedelta):
            delay = delay.total_seconds()

        self.max_attempts = max_attempts
        self.delay = delay

        self.handler_meta = handler_meta

    def __call__(self, func: Union[CheckerWrapper, Checker]) -> CheckerWrapper:
        if isinstance(func, CheckerWrapper):
            func = func.meta.func

        if iscoroutinefunction(func) and not iscoroutinefunction(self._handler_func):
            raise ValueError(f"{self._handler_func!r} must be a coroutine if {func!r} is a coroutine")

        meta = CheckerMeta(func, self.max_attempts, self.delay)

        self._handler_checkers.add(meta)

        return meta.wrapper

    @property
    def _handler_func(self) -> Handler:
        return self.handler_meta.func

    @property
    def _handler_checkers(self) -> set[CheckerMeta]:
        return self.handler_meta.checkers

    def __repr__(self):
        args = ", ".join(map(repr, self))
        return f"{type(self).__name__}({args})"

    def __eq__(self, other: CheckDecorator) -> bool:
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> Iterable:
        yield from (
            self.handler_meta,
            self.max_attempts,
            self.delay,
        )
