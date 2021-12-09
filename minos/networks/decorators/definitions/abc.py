from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from asyncio import (
    iscoroutinefunction,
)
from functools import (
    partial,
)
from typing import (
    Awaitable,
    Callable,
    Final,
    Iterable,
    Optional,
    Protocol,
    Type,
    Union,
)

from ...exceptions import (
    MinosMultipleEnrouteDecoratorKindsException,
)
from ...requests import (
    Request,
    Response,
)
from .checkers import (
    Checker,
    CheckFn,
    EnrouteCheckDecorator,
)
from .kinds import (
    EnrouteDecoratorKind,
)

Handler = Callable[[Request], Union[Optional[Response], Awaitable[Optional[Response]]]]


class HandlerProtocol(Protocol):
    def __call__(self, request: Request) -> Union[Optional[Response], Awaitable[Optional[Response]]]:
        """TODO"""
        ...

    # noinspection PyPropertyDefinition
    @property
    def meta(self) -> HandlerFn:
        """TODO"""
        ...

    # noinspection PyPropertyDefinition
    @property
    def check(self) -> Type[EnrouteCheckDecorator]:
        """TODO"""
        ...


class HandlerFn:
    """TODO"""

    __base_func__: Handler
    __checkers__: set[Checker]
    __decorators: set[EnrouteDecorator]

    def __init__(self, fn, __decorators__=None, __checkers__=None):
        if __decorators__ is None:
            __decorators__ = set()
        if __checkers__ is None:
            __checkers__ = set()
        self.__base_func__ = fn
        self.__decorators__ = __decorators__
        self.__checkers__ = __checkers__

    @property
    def __call__(self) -> Handler:
        if iscoroutinefunction(self.__base_func__):

            async def _wrapper(*args, **kwargs) -> Optional[Response]:
                if not await CheckFn.check_async(self.__checkers__, *args, **kwargs):
                    raise Exception("TODO")
                return await self.__base_func__(*args, **kwargs)

        else:

            def _wrapper(*args, **kwargs) -> Optional[Response]:
                if not CheckFn.check_sync(self.__checkers__, *args, **kwargs):
                    raise Exception("TODO")
                return self.__base_func__(*args, **kwargs)

        return _wrapper

    def add_decorator(self, dec):
        """TODO"""
        self.__decorators__.add(dec)
        kinds = set(decorator.KIND for decorator in self.__decorators__)
        if len(kinds) > 1:
            raise MinosMultipleEnrouteDecoratorKindsException(
                f"There are multiple kinds but only one is allowed: {kinds}"
            )

    @property
    def check(self) -> Type[EnrouteCheckDecorator]:
        """TODO"""
        # noinspection PyTypeChecker
        return partial(EnrouteCheckDecorator, _checkers=self.__checkers__, _base=self.__base_func__)


class EnrouteDecorator(ABC):
    """Base Decorator class."""

    # noinspection PyFinal
    KIND: Final[EnrouteDecoratorKind]

    def __call__(self, fn: Handler) -> HandlerProtocol:
        if hasattr(fn, "meta"):
            fn = fn.meta

        if not isinstance(fn, HandlerFn):
            fn = HandlerFn(fn)

        fn.add_decorator(self)

        def _wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        _wrapper.meta = fn
        _wrapper.check = fn.check

        return _wrapper

    def __repr__(self):
        args = ", ".join(map(repr, self))
        return f"{type(self).__name__}({args})"

    def __eq__(self, other: EnrouteDecorator) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    @abstractmethod
    def __iter__(self) -> Iterable:
        raise NotImplementedError

    @property
    def pre_fn_name(self) -> str:
        """Get the pre execution function name.

        :return: A string value containing the function name.
        """
        return self.KIND.pre_fn_name

    @property
    def post_fn_name(self) -> str:
        """Get the post execution function name.

        :return: A string value containing the function name.
        """
        return self.KIND.post_fn_name
