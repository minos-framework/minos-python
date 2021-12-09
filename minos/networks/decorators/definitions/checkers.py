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
from inspect import (
    iscoroutinefunction,
)
from typing import (
    TYPE_CHECKING,
    Optional,
    Protocol,
    Union,
)

from ...requests import (
    Request,
)

if TYPE_CHECKING:
    from .abc import (
        Handler,
    )

Checker = Callable[[Request], Union[Optional[bool], Awaitable[Optional[bool]]]]


class CheckerProtocol(Checker, Protocol):
    """TODO"""

    def __call__(self, request: Request) -> Union[Optional[bool], Awaitable[Optional[bool]]]:
        """TODO"""
        ...

    # noinspection PyPropertyDefinition
    @property
    def meta(self) -> CheckerFn:
        """TODO"""
        ...


class CheckerFn(Checker):
    """TODO"""

    def __init__(
        self, base: Checker, attempts: int, each: float, decorators: Optional[set[EnrouteCheckDecorator]] = None
    ):
        if decorators is None:
            decorators = set()
        self.base = base
        self.attempts = attempts
        self.each = each
        self.decorators = decorators

    @property
    def __call__(self):
        if iscoroutinefunction(self.base):

            async def _wrapper(*args, **kwargs) -> bool:
                r = 0
                while r < self.attempts and not await self.base(*args, **kwargs):
                    await asyncio.sleep(self.each)
                    r += 1
                return r < self.attempts

        else:

            def _wrapper(*args, **kwargs) -> bool:
                r = 0
                while r < self.attempts and not self.base(*args, **kwargs):
                    time.sleep(self.each)
                    r += 1
                return r < self.attempts

        return _wrapper

    def add_decorator(self, decorator: EnrouteCheckDecorator) -> None:
        """TODO

        :param decorator: TODO
        :return: TODO
        """
        self.decorators.add(decorator)

    @staticmethod
    async def check_async(checkers: set[Checker], *args, **kwargs) -> bool:
        """TODO

        :param checkers: TODO
        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        fns = list()
        for checker in checkers:
            if not iscoroutinefunction(checker):

                async def _fn(*ag, **kwg):
                    return checker(*ag, **kwg)

                fns.append(_fn)
            else:
                fns.append(checker)

        if not all(await gather(*(_c(*args, **kwargs) for _c in fns))):
            return False
        return True

    @staticmethod
    def check_sync(checkers: set[Checker], *args, **kwargs) -> bool:
        """TODO

        :param checkers: TODO
        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        for checker in checkers:
            if not checker(*args, **kwargs):
                return False
        return True


class EnrouteCheckDecorator:
    """TODO"""

    def __init__(
        self,
        each: float = 0.1,
        attempts: int = 10,
        _checkers: Optional[set[Checker]] = None,
        _base: Optional[Handler] = None,
    ):
        self.each = each
        self.attempts = attempts
        self._checkers = _checkers
        self._base = _base

    def __iter__(self) -> Iterable:
        yield from (
            self.each,
            self.attempts,
        )

    def __call__(self, fn: Checker) -> CheckerProtocol:
        if iscoroutinefunction(fn) and not iscoroutinefunction(self._base):
            raise Exception(f"{self._base!r} must be a coroutine if {fn!r} is a coroutine")

        if hasattr(fn, "meta"):
            fn = fn.meta

        if not isinstance(fn, CheckerFn):
            fn = CheckerFn(fn, self.attempts, self.each)

        fn.add_decorator(self)

        def _wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        _wrapper.meta = fn

        if self._checkers is not None:
            self._checkers.add(_wrapper)

        return _wrapper
