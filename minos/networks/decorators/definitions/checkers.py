import asyncio
import time
from collections.abc import (
    Awaitable,
    Callable,
    Iterable,
)
from inspect import (
    iscoroutinefunction,
)
from typing import (
    Optional,
    Union,
)

from ...requests import (
    Request,
)

Checker = Callable[[Request], Union[Optional[bool], Awaitable[Optional[bool]]]]


class EnrouteCheckDecorator:
    """TODO"""

    def __init__(self, each: int = 100, attempts: int = 10, _checkers=None, _base=None):
        self.each = each
        self.attempts = attempts
        self._checkers = _checkers
        self._base = _base

    def __iter__(self) -> Iterable:
        yield from (
            self.each,
            self.attempts,
        )

    def __call__(self, fn: Checker) -> Checker:
        base_checker = getattr(fn, "__base_func__", fn)

        if iscoroutinefunction(fn):
            if not iscoroutinefunction(self._base):
                raise Exception(f"{self._base!r} must be a coroutine if {base_checker!r} is a coroutine")

            async def _wrapper(*args, **kwargs) -> bool:
                r = 0
                while r < self.attempts and not await _wrapper.__base_func__(*args, **kwargs):
                    await asyncio.sleep(self.each)
                    r += 1
                return r < self.attempts

        else:

            def _wrapper(*args, **kwargs) -> bool:
                r = 0
                while r < self.attempts and not _wrapper.__base_func__(*args, **kwargs):
                    time.sleep(self.each)
                    r += 1
                return r < self.attempts

        _wrapper.__check_decorators__ = getattr(fn, "___check_decorators__", set())
        _wrapper.__check_decorators__.add(self)
        _wrapper.__base_func__ = base_checker

        self._checkers.add(_wrapper)

        return _wrapper
