from __future__ import (
    annotations,
)

import asyncio
import time
from asyncio import (
    gather,
    iscoroutinefunction,
)
from collections.abc import (
    Awaitable,
    Callable,
    Iterable,
)
from functools import (
    wraps,
)
from inspect import (
    isawaitable,
)
from typing import (
    Optional,
    Protocol,
    Union,
    runtime_checkable,
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


@runtime_checkable
class CheckerWrapper(Protocol):
    """TODO"""

    meta: CheckerMeta
    __call__: Checker


class CheckerMeta:
    """TODO"""

    func: Checker
    max_attempts: int
    delay: float

    def __init__(self, func: Checker, max_attempts: int, delay: float):
        self.func = func
        self.max_attempts = max_attempts
        self.delay = delay

    @staticmethod
    async def run_async(metas: set[CheckerMeta], *args, **kwargs) -> None:
        """TODO

        :param metas: TODO
        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        metas = list(metas)
        futures = [meta.async_wrapper(*args, **kwargs) for meta in metas]

        for satisfied, meta in zip(await gather(*futures), metas):
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
            satisfied = meta.sync_wrapper(*args, **kwargs)
            if not satisfied:
                raise NotSatisfiedCheckerException(f"{meta.wrapper!r} is not satisfied.")
        return True

    @property
    def wrapper(self) -> CheckerWrapper:
        """TODO

        :return: TODO
        """
        if iscoroutinefunction(self.func):
            return self.async_wrapper
        else:
            return self.sync_wrapper

    @cached_property
    def async_wrapper(self) -> CheckerWrapper:
        """TODO

        :return: TODO
        """

        @wraps(self.func)
        async def _wrapper(*args, **kwargs) -> bool:
            r = 0
            while r < self.max_attempts:
                satisfied = self.func(*args, **kwargs)
                if isawaitable(satisfied):
                    satisfied = await satisfied
                if satisfied:
                    return True

                await asyncio.sleep(self.delay)
                r += 1

            return False

        _wrapper.meta = self
        return _wrapper

    @cached_property
    def sync_wrapper(self) -> CheckerWrapper:
        """TODO

        :return: TODO
        """
        if iscoroutinefunction(self.func):
            raise ValueError(f"{self.func!r} cannot be awaitable.")

        @wraps(self.func)
        def _wrapper(*args, **kwargs) -> bool:
            r = 0
            while r < self.max_attempts:
                satisfied = self.func(*args, **kwargs)
                if satisfied:
                    return True

                time.sleep(self.delay)
                r += 1

            return False

        _wrapper.meta = self
        return _wrapper

    def __repr__(self):
        args = ", ".join(map(repr, self))
        return f"{type(self).__name__}({args})"

    def __eq__(self, other: CheckerMeta) -> bool:
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> Iterable:
        yield from (
            self.func,
            self.max_attempts,
            self.delay,
        )
