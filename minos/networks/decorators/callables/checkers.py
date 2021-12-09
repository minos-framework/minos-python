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
)
from functools import (
    wraps,
)
from inspect import (
    iscoroutinefunction,
)
from typing import (
    Optional,
    Protocol,
    Union,
)

from cached_property import (
    cached_property,
)

from ...exceptions import (
    NotSatisfiedCheckerException,
)
from ...requests import (
    Request,
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
    async def run_async(metas: set[CheckerMeta], *args, **kwargs) -> None:
        """TODO

        :param metas: TODO
        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        fns = list()
        for meta in metas:
            if iscoroutinefunction(meta.wrapper):
                fns.append(meta.wrapper)
            else:

                @wraps(meta.wrapper)
                async def _wrapper(*ag, **kwg):
                    return meta.wrapper(*ag, **kwg)

                fns.append(_wrapper)

        for satisfied, meta in zip(await gather(*(coro(*args, **kwargs) for coro in fns)), metas):
            if not satisfied:
                raise NotSatisfiedCheckerException(f"{meta.wrapper!r} is not satisfied.")

    @staticmethod
    def run_sync(metas: set[CheckerMeta], *args, **kwargs) -> bool:
        """TODO

        :param metas: TODO
        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        for meta in metas:
            satisfied = meta.wrapper(*args, **kwargs)
            if not satisfied:
                raise NotSatisfiedCheckerException(f"{meta.wrapper!r} is not satisfied.")
        return True
