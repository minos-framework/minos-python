"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
import typing as t

from ..types import (
    ModelRef,
)
from .utils import (
    _is_model_cls,
)

logger = logging.getLogger(__name__)

T = t.TypeVar("T")


class TypeHintComparator:
    """TODO"""

    def __init__(self, first: t.Type, second: t.Type):
        self._first = first
        self._second = second

    def match(self) -> bool:
        """Build the avro schema for the given field.

        :return: A dictionary object.
        """

        return self._compare(self._first, self._second)

    def _compare(self, first: t.Type, second: t.Type) -> bool:
        if t.get_origin(first) is ModelRef:
            tt = (*t.get_args(first), int)
            first = t.Union[tt]
        if t.get_origin(second) is ModelRef:
            tt = (*t.get_args(second), int)
            second = t.Union[tt]

        if _is_model_cls(first):
            first = first.model_type

        if _is_model_cls(second):
            second = second.model_type

        if first == second:
            return True

        first_origin, second_origin = t.get_origin(first), t.get_origin(second)

        if first_origin is not None and self._compare(first_origin, second_origin):
            return self._compare_args(first, second)
        return False

    def _compare_args(self, first: t.Type, second: t.Type) -> bool:
        f1, f2 = t.get_args(first), t.get_args(second)
        if len(f1) != len(f2):
            return False
        alternatives = zip(f1, f2)
        return all(self._compare(f1, f2) for f1, f2 in alternatives)
