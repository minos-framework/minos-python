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

logger = logging.getLogger(__name__)

T = t.TypeVar("T")


class TypeHintBuilder:
    """Avro Schema Decoder class."""

    def __init__(self, value, base=None):
        self._value = value
        self._base = base

    def build(self) -> t.Type[T]:
        """Build type from given avro schema item.

        :return: A dictionary object.
        """
        built_type = self._compare(self._value, self._base)
        return built_type

    def _compare(self, value, base) -> t.Type[T]:
        if base is not None and t.get_origin(base) is t.Union:
            v = self._compare(value, None)
            already = set()
            vs = list()
            for a in t.get_args(base):
                if a in already:
                    continue
                if v not in already and (not len(t.get_args(a)) and issubclass(v, a)):
                    a = v
                vs.append(a)
                already.add(a)

            return t.Union[tuple(vs)]
        if isinstance(value, list):
            b1 = None
            if base is not None:
                b1 = t.get_args(base)[0]
            return list[self._build_from_iterable(value, b1)]
        if isinstance(value, dict):
            b1, b2 = None, None
            if base is not None:
                b1, b2 = t.get_args(base)
            return dict[self._build_from_iterable(value.keys(), b1), self._build_from_iterable(value.values(), b2)]
        if hasattr(value, "model_type"):
            return value.model_type
        return type(value)

    def _build_from_iterable(self, values, base) -> t.Type[T]:
        return t.Union[tuple(set(self._compare(value, base) for value in values))]
