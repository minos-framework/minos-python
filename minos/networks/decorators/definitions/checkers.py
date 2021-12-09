from __future__ import (
    annotations,
)

import asyncio
import time
from asyncio import (
    gather,
)
from collections.abc import (
    Awaitable,
    Callable,
    Iterable,
)
from datetime import (
    timedelta,
)
from functools import (
    wraps,
)
from inspect import (
    iscoroutinefunction,
)
from typing import (
    TYPE_CHECKING,
    Optional,
    Protocol,
    Union,
)

from cached_property import (
    cached_property,
)

from ...requests import (
    Request,
)

if TYPE_CHECKING:
    from .abc import (
        Handler,
    )

Checker = Callable[[Request], Union[Optional[bool], Awaitable[Optional[bool]]]]


class CheckerProtocol(Protocol):
    """TODO"""

    meta: CheckerMeta
    __call__: Checker


class CheckerMeta:
    """TODO"""

    def __init__(self, base: Checker, attempts: int, delay: float):
        self.base = base
        self.max_attempts = attempts
        self.delay = delay

    @cached_property
    def wrapper(self) -> CheckerProtocol:
        """TODO

        :return: TODO
        """
        if iscoroutinefunction(self.base):

            @wraps(self.base)
            async def _wrapper(*args, **kwargs) -> bool:
                r = 0
                while r < self.max_attempts and not await self.base(*args, **kwargs):
                    await asyncio.sleep(self.delay)
                    r += 1
                return r < self.max_attempts

        else:

            @wraps(self.base)
            def _wrapper(*args, **kwargs) -> bool:
                r = 0
                while r <= self.max_attempts and not self.base(*args, **kwargs):
                    time.sleep(self.delay)
                    r += 1
                return r <= self.max_attempts

        _wrapper.meta = self
        return _wrapper

    @staticmethod
    async def check_async(checkers: set[CheckerMeta], *args, **kwargs) -> bool:
        """TODO

        :param checkers: TODO
        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        fns = list()
        for meta in checkers:
            if iscoroutinefunction(meta.wrapper):
                fns.append(meta.wrapper)
            else:

                @wraps(meta.wrapper)
                async def _wrapper(*ag, **kwg):
                    return meta.wrapper(*ag, **kwg)

                fns.append(_wrapper)

        if not all(await gather(*(coro(*args, **kwargs) for coro in fns))):
            return False
        return True

    @staticmethod
    def check_sync(checkers: set[CheckerMeta], *args, **kwargs) -> bool:
        """TODO

        :param checkers: TODO
        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        for meta in checkers:
            if not meta.wrapper(*args, **kwargs):
                return False
        return True


class EnrouteCheckDecorator:
    """TODO"""

    def __init__(
        self,
        max_attempts: int = 10,
        delay: Union[float, timedelta] = 0.1,
        _checkers: Optional[set[CheckerMeta]] = None,
        _base: Optional[Handler] = None,
    ):
        if isinstance(delay, timedelta):
            delay = delay.total_seconds()

        self.max_attempts = max_attempts
        self.delay = delay

        self._checkers = _checkers
        self._base = _base

    def __iter__(self) -> Iterable:
        yield from (
            self.delay,
            self.max_attempts,
        )

    def __call__(self, meta: Checker) -> CheckerProtocol:
        if not isinstance(meta, CheckerMeta):
            meta = getattr(meta, "meta", CheckerMeta(meta, self.max_attempts, self.delay))

        if iscoroutinefunction(meta) and not iscoroutinefunction(self._base):
            raise Exception(f"{self._base!r} must be a coroutine if {meta!r} is a coroutine")

        if self._checkers is not None:
            self._checkers.add(meta)

        return meta.wrapper
