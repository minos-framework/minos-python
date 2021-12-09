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
from collections.abc import (
    Awaitable,
    Callable,
    Iterable,
)
from functools import (
    partial,
)
from typing import (
    Final,
    Optional,
    Protocol,
    Type,
    Union,
)

from cached_property import (
    cached_property,
)

from ...exceptions import (
    MinosMultipleEnrouteDecoratorKindsException,
)
from ...requests import (
    Request,
    Response,
)
from .checkers import (
    CheckerMeta,
    EnrouteCheckDecorator,
)
from .kinds import (
    EnrouteDecoratorKind,
)

Handler = Callable[[Request], Union[Optional[Response], Awaitable[Optional[Response]]]]


class HandlerProtocol(Protocol):
    """TODO"""

    meta: HandlerMeta
    check: Type[EnrouteCheckDecorator]
    __call__: Handler


class HandlerMeta:
    """TODO"""

    base: Handler
    decorators: set[EnrouteDecorator]
    checkers: set[CheckerMeta]

    def __init__(
        self,
        base: Handler,
        decorators: Optional[set[EnrouteDecorator]] = None,
        checkers: Optional[set[CheckerMeta]] = None,
    ):
        if decorators is None:
            decorators = set()
        if checkers is None:
            checkers = set()
        self.base = base
        self.decorators = decorators
        self.checkers = checkers

    @cached_property
    def wrapper(self) -> HandlerProtocol:
        """TODO

        :return: TODO
        """
        if iscoroutinefunction(self.base):

            async def _wrapper(*args, **kwargs) -> Optional[Response]:
                if not await CheckerMeta.check_async(self.checkers, *args, **kwargs):
                    raise Exception("TODO")
                return await self.base(*args, **kwargs)

        else:

            def _wrapper(*args, **kwargs) -> Optional[Response]:
                if not CheckerMeta.check_sync(self.checkers, *args, **kwargs):
                    raise Exception("TODO")
                return self.base(*args, **kwargs)

        _wrapper.meta = self
        _wrapper.check = self.check

        return _wrapper

    def add_decorator(self, decorator: EnrouteDecorator) -> None:
        """TODO

        :param decorator: TODO
        :return: TODO
        """
        another = next(iter(self.decorators), None)
        if another is not None and another.KIND != decorator.KIND:
            raise MinosMultipleEnrouteDecoratorKindsException(
                f"There are multiple kinds but only one is allowed: {(another.KIND, decorator.KIND)}"
            )
        self.decorators.add(decorator)

    @property
    def check(self) -> Type[EnrouteCheckDecorator]:
        """TODO

        :return: TODO
        """
        # noinspection PyTypeChecker
        return partial(EnrouteCheckDecorator, _checkers=self.checkers, _base=self.base)


class EnrouteDecorator(ABC):
    """Base Decorator class."""

    # noinspection PyFinal
    KIND: Final[EnrouteDecoratorKind]

    def __call__(self, meta: Handler) -> HandlerProtocol:
        if not isinstance(meta, HandlerMeta):
            meta = getattr(meta, "meta", HandlerMeta(meta))

        meta.add_decorator(self)

        return meta.wrapper

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
