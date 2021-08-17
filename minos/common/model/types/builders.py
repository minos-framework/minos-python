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
    Iterable,
    Optional,
    Union,
    get_args,
    get_origin,
)
from uuid import (
    UUID,
)

from .comparators import (
    is_aggregate_type,
    is_model_type,
    is_type_subclass,
)
from .model_refs import (
    ModelRef,
)
from .model_types import (
    ModelType,
)

logger = logging.getLogger(__name__)


def build_union(options: tuple[type, ...]) -> type:
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


class TypeHintBuilder:
    """Type Hint Builder class."""

    def __init__(self, value: Any, type_: Optional[type] = None):
        self.value = value
        self.type_ = type_

    def build(self) -> type:
        """Build type hint from an instance..

        :return: A type.
        """
        return self._build(self.value, self.type_)

    def _build(self, value, type_: Optional[type]) -> type:
        if type_ is not None:
            if get_origin(type_) is ModelRef:
                type_ = Union[(*get_args(type_), UUID)]

            if get_origin(type_) is Union:
                dynamic = self._build(value, None)
                options = tuple(
                    (dynamic if not len(get_args(static)) and issubclass(dynamic, static) else static)
                    for static in get_args(type_)
                )
                return build_union(options)

        if isinstance(value, (tuple, list, set)):
            b1 = Any if (type_ is None or len(get_args(type_)) != 1) else get_args(type_)[0]
            return type(value)[self._build_from_iterable(value, b1)]

        if isinstance(value, dict):
            b1, b2 = (Any, Any) if (type_ is None or len(get_args(type_)) != 2) else get_args(type_)
            return type(value)[
                self._build_from_iterable(value.keys(), b1), self._build_from_iterable(value.values(), b2)
            ]

        if is_model_type(value):
            return ModelType.from_model(value)

        return type(value)

    def _build_from_iterable(self, values: Iterable, type_: Optional[type]) -> type:
        values = tuple(values)
        if len(values) == 0:
            return type_

        options = tuple(self._build(value, type_) for value in values)
        return build_union(options)
