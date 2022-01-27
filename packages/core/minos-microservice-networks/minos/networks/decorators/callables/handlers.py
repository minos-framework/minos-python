from __future__ import (
    annotations,
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
    wraps,
)
from inspect import (
    isawaitable,
)
from typing import (
    TYPE_CHECKING,
    Optional,
    Protocol,
    Type,
    Union,
    runtime_checkable,
)

from cached_property import (
    cached_property,
)

from ...exceptions import (
    MinosMultipleEnrouteDecoratorKindsException,
    NotSatisfiedCheckerException,
)
from ...requests import (
    Request,
    Response,
    ResponseException,
)
from .checkers import (
    CheckerMeta,
)

if TYPE_CHECKING:
    from ..definitions import (
        CheckDecorator,
        EnrouteDecorator,
    )

Handler = Callable[[Request], Union[Optional[Response], Awaitable[Optional[Response]]]]


@runtime_checkable
class HandlerWrapper(Protocol):
    """Handler Wrapper class."""

    meta: HandlerMeta
    check: Type[CheckDecorator]
    __call__: Handler


class HandlerMeta:
    """Handler Meta class."""

    func: Handler
    decorators: set[EnrouteDecorator]
    checkers: set[CheckerMeta]

    def __init__(
        self,
        func: Handler,
        decorators: Optional[set[EnrouteDecorator]] = None,
        checkers: Optional[set[CheckerMeta]] = None,
    ):
        if decorators is None:
            decorators = set()
        if checkers is None:
            checkers = set()
        self.func = func
        self.decorators = decorators
        self.checkers = checkers

    @property
    def wrapper(self) -> HandlerWrapper:
        """Get the ``HandlerWrapper`` instance.

        :return: A ``HandlerWrapper`` instance.
        """
        if iscoroutinefunction(self.func):
            return self.async_wrapper
        else:
            return self.sync_wrapper

    @cached_property
    def async_wrapper(self) -> HandlerWrapper:
        """Get the async ``HandlerWrapper`` instance.

        :return: A ``HandlerWrapper`` instance.
        """

        @wraps(self.func)
        async def _wrapper(*args, **kwargs) -> Optional[Response]:
            try:
                await CheckerMeta.run_async(self.checkers, *args, **kwargs)
            except NotSatisfiedCheckerException as exc:
                raise ResponseException(f"There was an exception during check step: {exc}")

            response = self.func(*args, **kwargs)
            if isawaitable(response):
                response = await response
            return response

        _wrapper.meta = self
        _wrapper.check = self.check
        _wrapper.__decorators__ = self.decorators  # FIXME: This attribute should be removed in future versions.

        return _wrapper

    @cached_property
    def sync_wrapper(self) -> HandlerWrapper:
        """Get the sync ``HandlerWrapper`` instance.

        :return: A ``HandlerWrapper`` instance.
        """

        if iscoroutinefunction(self.func):
            raise ValueError(f"{self.func!r} cannot be awaitable.")

        @wraps(self.func)
        def _wrapper(*args, **kwargs) -> Optional[Response]:
            try:
                CheckerMeta.run_sync(self.checkers, *args, **kwargs)
            except NotSatisfiedCheckerException as exc:
                raise ResponseException(f"There was an exception during check step: {exc}")

            return self.func(*args, **kwargs)

        _wrapper.meta = self
        _wrapper.check = self.check
        _wrapper.__decorators__ = self.decorators  # FIXME: This attribute should be removed in future versions.

        return _wrapper

    def add_decorator(self, decorator: EnrouteDecorator) -> None:
        """Add a new decorator to the ``decorators`` set.

        :param decorator: The decorator to be added.
        :return: This method does not return anything.
        """
        another = next(iter(self.decorators), None)
        if another is not None and another.KIND != decorator.KIND:
            raise MinosMultipleEnrouteDecoratorKindsException(
                f"There are multiple kinds but only one is allowed: {(another.KIND, decorator.KIND)}"
            )
        self.decorators.add(decorator)

    @cached_property
    def check(self) -> Type[CheckDecorator]:
        """Get the check decorator.

        :return: A ``CheckDecorator`` type.
        """
        from ..definitions import (
            CheckDecorator,
        )

        # noinspection PyTypeChecker
        return partial(CheckDecorator, handler_meta=self)

    def __repr__(self):
        args = ", ".join(map(repr, self))
        return f"{type(self).__name__}({args})"

    def __eq__(self, other: CheckerMeta) -> bool:
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(self.func)

    def __iter__(self) -> Iterable:
        yield from (
            self.func,
            self.decorators,
            self.checkers,
        )
