from __future__ import (
    annotations,
)

import logging
from functools import (
    lru_cache,
)
from typing import (
    Any,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

from .generics import (
    unpack_typevar,
)
from .model_types import (
    ModelType,
)


def is_model_subclass(type_: type) -> bool:
    """Check if the given type field is subclass of ``Model``."""
    from ..abc import (
        Model,
    )

    if not is_type_subclass(type_):
        type_ = get_origin(type_)
    return is_type_subclass(type_) and issubclass(type_, Model)


def is_type_subclass(type_: type) -> bool:
    """Check if the given type field is subclass of ``type``."""
    return issubclass(type(type_), type(type))


def is_model_type(value_: type):
    """Check if the given type is a model instance."""
    from ..abc import (
        Model,
    )

    return isinstance(value_, Model)


logger = logging.getLogger(__name__)

T = TypeVar("T", bound=type)
K = TypeVar("K", bound=type)


class TypeHintComparator:
    """Type Hint Comparator class."""

    def __init__(self, first: T, second: K):
        self._first = first
        self._second = second

    def match(self) -> bool:
        """Check if the two types match.

        :return: ``True`` if there is a match or ``False`` otherwise.
        """

        return self._compare(self._first, self._second)

    @classmethod
    @lru_cache()
    def _compare(cls, first: T, second: K) -> bool:
        if isinstance(first, TypeVar):
            first = unpack_typevar(first)

        if isinstance(second, TypeVar):
            second = unpack_typevar(second)

        if second is Any:
            return True

        if get_origin(first) is Union and all(cls._compare(f, second) for f in get_args(first)):
            return True

        if get_origin(second) is Union and any(cls._compare(first, s) for s in get_args(second)):
            return True

        if is_model_subclass(first):
            first = ModelType.from_model(first)

        if is_model_subclass(second):
            second = ModelType.from_model(second)

        if first == second:
            return True

        if is_type_subclass(first) and is_type_subclass(second):
            if isinstance(first, ModelType) and isinstance(second, ModelType):
                if first <= second:
                    return True
            elif issubclass(first, second):
                return True

        first_origin, second_origin = get_origin(first), get_origin(second)
        if first_origin is not None and cls._compare(first_origin, second_origin):
            return cls._compare_args(first, second)

        return False

    @classmethod
    def _compare_args(cls, first: T, second: K) -> bool:
        first_args, second_args = get_args(first), get_args(second)
        if len(first_args) != len(second_args):
            return False
        return all(cls._compare(fi, si) for fi, si in zip(first_args, second_args))
