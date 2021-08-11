"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    Any,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)


# noinspection SpellCheckingInspection
def unpack_typevar(value: TypeVar) -> Type:
    """TODO

    :param value: TODO
    :return: TODO
    """
    return Union[value.__constraints__ or (value.__bound__ or Any,)]


class GenericTypeProjector:
    """TODO"""

    def __init__(self, type_hints: dict[str, Type], mapper: dict[TypeVar, Type]):
        self.type_hints = type_hints
        self.mapper = mapper

    @classmethod
    def from_model(cls, type_) -> GenericTypeProjector:
        """TODO

        :param type_: TODO
        :return: TODO
        """
        generics_ = dict(zip(type_.type_hints_parameters, get_args(type_)))
        return cls(type_.type_hints, generics_)

    def build(self) -> dict[str, Type]:
        """TODO

        :return: TODO
        """
        return {k: self._build(v) for k, v in self.type_hints.items()}

    def _build(self, value):
        if isinstance(value, TypeVar):
            return self.mapper.get(value, unpack_typevar(value))

        origin = get_origin(value)
        if origin is None:
            return value

        return self._build(origin)[tuple(self._build(arg) for arg in get_args(value))]
