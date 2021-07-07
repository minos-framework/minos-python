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
    Generic,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

from ..types import (
    ModelRef,
)
from .utils import (
    _is_model_cls,
    _is_type,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")
K = TypeVar("K")


class TypeHintComparator(Generic[T, K]):
    """Type Hint Comparator class."""

    def __init__(self, first: Type[T], second: Type[K]):
        self._first = first
        self._second = second

    def match(self) -> bool:
        """Check if the two types match.

        :return: ``True`` if there is a match or ``False`` otherwise.
        """

        return self._compare(self._first, self._second)

    def _compare(self, first: Type[T], second: Type[K]) -> bool:
        if get_origin(first) is ModelRef:
            first = Union[(*get_args(first), int)]

        if get_origin(second) is ModelRef:
            second = Union[(*get_args(second), int)]

        if _is_type(first) and _is_model_cls(first):
            first = first.model_type

        if _is_type(second) and _is_model_cls(second):
            second = second.model_type

        if first == second:
            return True

        first_origin, second_origin = get_origin(first), get_origin(second)
        if first_origin is not None and self._compare(first_origin, second_origin):
            return self._compare_args(first, second)

        return False

    def _compare_args(self, first: Type[T], second: Type[K]) -> bool:
        first_args, second_args = get_args(first), get_args(second)
        if len(first_args) != len(second_args):
            return False
        return all(self._compare(fi, si) for fi, si in zip(first_args, second_args))
