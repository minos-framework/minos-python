from __future__ import (
    annotations,
)

from asyncio import (
    iscoroutinefunction,
)
from collections.abc import (
    Awaitable,
    Callable,
)
from functools import (
    partial,
    wraps,
)
from typing import (
    TYPE_CHECKING,
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
)

if TYPE_CHECKING:
    from ..definitions import (
        EnrouteCheckDecorator,
        EnrouteDecorator,
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

            @wraps(self.base)
            async def _wrapper(*args, **kwargs) -> Optional[Response]:
                if not await CheckerMeta.run_async(self.checkers, *args, **kwargs):
                    raise Exception("TODO")
                return await self.base(*args, **kwargs)

        else:

            @wraps(self.base)
            def _wrapper(*args, **kwargs) -> Optional[Response]:
                if not CheckerMeta.run_sync(self.checkers, *args, **kwargs):
                    raise Exception("Some checks are not satisfied.")
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
        from ..definitions import (
            EnrouteCheckDecorator,
        )

        # noinspection PyTypeChecker
        return partial(EnrouteCheckDecorator, _checkers=self.checkers, _base=self.base)
