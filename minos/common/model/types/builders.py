"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from typing import (
    Any,
    Generic,
    Iterable,
    Optional,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)
from uuid import (
    UUID,
)

from .comparators import (
    is_aggregate_type,
    is_type_subclass,
)
from .model_refs import (
    ModelRef,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


def build_union(options: tuple[Type[T], ...]) -> Type[T]:
    """Build the union type base on the given options.

    :param options: A tuple of types.
    :return: The union of types.
    """
    if (
        len(options) == 2
        and is_aggregate_type(options[0])
        and is_type_subclass(options[1])
        and issubclass(options[1], UUID)
    ):
        return ModelRef[options[0]]

    return Union[options]


class TypeHintBuilder(Generic[T]):
    """Type Hint Builder class."""

    def __init__(self, value: T, base: Optional[Type[T]] = None):
        self._value = value
        self._base = base

    def build(self) -> Type[T]:
        """Build type hint from an instance..

        :return: A type.
        """
        return self._build(self._value, self._base)

    def _build(self, value: T, base: Optional[Type[T]]) -> Type[T]:
        if base is not None:
            if get_origin(base) is ModelRef:
                base = Union[(*get_args(base), UUID)]

            if get_origin(base) is Union:
                dynamic: Type[T] = self._build(value, None)
                options = tuple(
                    (dynamic if not len(get_args(static)) and issubclass(dynamic, static) else static)
                    for static in get_args(base)
                )
                return build_union(options)

        if isinstance(value, (tuple, list, set)):
            b1 = Any if (base is None or len(get_args(base)) != 1) else get_args(base)[0]
            return type(value)[self._build_from_iterable(value, b1)]

        if isinstance(value, dict):
            b1, b2 = (Any, Any) if (base is None or len(get_args(base)) != 2) else get_args(base)
            return type(value)[
                self._build_from_iterable(value.keys(), b1), self._build_from_iterable(value.values(), b2)
            ]

        if hasattr(value, "model_type"):
            return value.model_type

        return type(value)

    def _build_from_iterable(self, values: Iterable[T], base: Optional[Type[T]]) -> Type[T]:
        values = tuple(values)
        if len(values) == 0:
            return base

        options = tuple(self._build(value, base) for value in values)
        return build_union(options)
