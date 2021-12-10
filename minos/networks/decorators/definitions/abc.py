from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    Iterable,
)
from typing import (
    Final,
)

from ..callables import (
    Handler,
    HandlerMeta,
    HandlerWrapper,
)
from .kinds import (
    EnrouteHandleDecoratorKind,
)


class EnrouteHandleDecorator(ABC):
    """Base Decorator class."""

    # noinspection PyFinal
    KIND: Final[EnrouteHandleDecoratorKind]

    def __call__(self, func: Handler) -> HandlerWrapper:
        meta = getattr(func, "meta", HandlerMeta(func))

        meta.add_decorator(self)

        return meta.wrapper

    def __repr__(self):
        args = ", ".join(map(repr, self))
        return f"{type(self).__name__}({args})"

    def __eq__(self, other: EnrouteHandleDecorator) -> bool:
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
