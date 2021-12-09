from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from asyncio import (
    gather,
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
    EnrouteCheckDecorator,
)
from .kinds import (
    EnrouteDecoratorKind,
)

Adapter = Callable[[Request], Union[Optional[Response], Awaitable[Optional[Response]]]]


class HandlingFn(Protocol):
    """TODO"""

    def __call__(self, request: Request) -> Union[Optional[Response], Awaitable[Optional[Response]]]:
        """TODO"""
        ...

    # noinspection PyPropertyDefinition
    @property
    def __decorators__(self) -> set[EnrouteDecorator]:
        """TODO"""
        ...

    # noinspection PyPropertyDefinition
    @property
    def __base_func__(self) -> Adapter:
        """TODO"""
        ...

    # noinspection PyPropertyDefinition
    @property
    def check(self) -> Type[EnrouteCheckDecorator]:
        """TODO"""
        ...


class EnrouteDecorator(ABC):
    """Base Decorator class."""

    # noinspection PyFinal
    KIND: Final[EnrouteDecoratorKind]

    def __call__(self, fn: Adapter) -> HandlingFn:
        if iscoroutinefunction(fn):

            async def _wrapper(*args, **kwargs) -> Optional[Response]:
                fns = list()
                for checker in _wrapper.__checkers__:
                    if not iscoroutinefunction(checker):

                        async def _fn(*args, **kwargs):
                            return checker(*args, **kwargs)

                        fns.append(_fn)
                    else:
                        fns.append(checker)

                if not all(await gather(*(_c(*args, **kwargs) for _c in fns))):
                    raise Exception("TODO")

                return await _wrapper.__base_func__(*args, **kwargs)

        else:

            def _wrapper(*args, **kwargs) -> Optional[Response]:
                for checker in _wrapper.__checkers__:
                    if not checker(*args, **kwargs):
                        raise Exception("TODO")

                return _wrapper.__base_func__(*args, **kwargs)

        _wrapper.__decorators__ = getattr(fn, "__decorators__", set())
        _wrapper.__decorators__.add(self)
        kinds = set(decorator.KIND for decorator in _wrapper.__decorators__)
        if len(kinds) > 1:
            raise MinosMultipleEnrouteDecoratorKindsException(
                f"There are multiple kinds but only one is allowed: {kinds}"
            )

        _wrapper.__base_func__ = getattr(fn, "__base_func__", fn)
        _wrapper.__checkers__ = getattr(fn, "__checkers__", set())
        _wrapper.check = partial(EnrouteCheckDecorator, _checkers=_wrapper.__checkers__, _base=_wrapper.__base_func__)

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
