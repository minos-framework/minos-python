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
    TypeVar,
    Union,
    get_args,
    get_origin,
)


# noinspection SpellCheckingInspection
def unpack_typevar(value: TypeVar) -> type:
    """Unpack `TypeVar` into a union of possible types.

    :param value: A type var instance.
    :return: A union of types.
    """
    return Union[value.__constraints__ or (value.__bound__ or Any,)]


class GenericTypeProjector:
    """Generic Type Projector."""

    def __init__(self, type_hints: dict[str, type], mapper: dict[TypeVar, type]):
        self.type_hints = type_hints
        self.mapper = mapper

    @classmethod
    def from_model(cls, type_: type) -> GenericTypeProjector:
        """Build a new instance from model.

        :param type_: The model class.
        :return: A ``GenericTypeProjector`` instance.
        """
        # noinspection PyUnresolvedReferences
        generics_ = dict(zip(type_.type_hints_parameters, get_args(type_)))
        # noinspection PyUnresolvedReferences
        return cls(type_.type_hints, generics_)

    def build(self) -> dict[str, type]:
        """Builder a projection of type vars values.

        :return: A dict of type hints.
        """
        return {k: self._build(v) for k, v in self.type_hints.items()}

    def _build(self, value: Any) -> type:
        if isinstance(value, TypeVar):
            return self.mapper.get(value, unpack_typevar(value))

        origin = get_origin(value)
        if origin is None:
            return value

        # noinspection PyUnresolvedReferences
        return self._build(origin)[tuple(self._build(arg) for arg in get_args(value))]
