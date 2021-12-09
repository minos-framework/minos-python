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
    CheckerFn,
    EnrouteCheckDecorator,
)
from .kinds import (
    EnrouteDecoratorKind,
)

Handler = Callable[[Request], Union[Optional[Response], Awaitable[Optional[Response]]]]


class HandlerProtocol(Protocol):
    """TODO"""

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

    base: Handler
    decorators: set[EnrouteDecorator]
    checkers: set[Checker]

    def __init__(self, fn, decorators: Optional[set[EnrouteDecorator]] = None, checkers: Optional[set[Checker]] = None):
        if decorators is None:
            decorators = set()
        if checkers is None:
            checkers = set()
        self.base = fn
        self.decorators = decorators
        self.checkers = checkers

    @property
    def __call__(self) -> Handler:
        if iscoroutinefunction(self.base):

            async def _wrapper(*args, **kwargs) -> Optional[Response]:
                if not await CheckerFn.check_async(self.checkers, *args, **kwargs):
                    raise Exception("TODO")
                return await self.base(*args, **kwargs)

        else:

            def _wrapper(*args, **kwargs) -> Optional[Response]:
                if not CheckerFn.check_sync(self.checkers, *args, **kwargs):
                    raise Exception("TODO")
                return self.base(*args, **kwargs)

        return _wrapper

    def add_decorator(self, decorator: EnrouteDecorator) -> None:
        """TODO"""
        another = next(iter(self.decorators), None)
        if another is not None and another.KIND != decorator.KIND:
            raise MinosMultipleEnrouteDecoratorKindsException(
                f"There are multiple kinds but only one is allowed: {(another.KIND, decorator.KIND)}"
            )
        self.decorators.add(decorator)

    @property
    def check(self) -> Type[EnrouteCheckDecorator]:
        """TODO"""
        # noinspection PyTypeChecker
        return partial(EnrouteCheckDecorator, _checkers=self.checkers, _base=self.base)


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
