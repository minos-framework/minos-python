from __future__ import (
    annotations,
)

import logging
from collections.abc import (
    Iterable,
)
from functools import (
    lru_cache,
)
from typing import (
    Any,
    Optional,
    Union,
    get_args,
    get_origin,
)

from .comparators import (
    TypeHintComparator,
    is_model_subclass,
    is_model_type,
)
from .model_types import (
    ModelType,
)

logger = logging.getLogger(__name__)


def build_union(options: Iterable[type, ...]) -> type:
    """Build the union type base on the given options.

    :param options: A tuple of types.
    :return: The union of types.
    """

    return Union[tuple(options)]


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
            if get_origin(type_) is Union:
                dynamic = self._build(value, None)
                options = tuple(self._build_from_dynamic(dynamic, static) for static in get_args(type_))
                return build_union(options)

        if isinstance(value, (tuple, list, set)):
            b1 = Any if (type_ is None or len(get_args(type_)) != 1) else get_args(type_)[0]
            return type(value)[self._build_from_iterable(value, b1)]

        if isinstance(value, dict):
            b1, b2 = (str, Any) if (type_ is None or len(get_args(type_)) != 2) else get_args(type_)
            return type(value)[
                self._build_from_iterable(value.keys(), b1), self._build_from_iterable(value.values(), b2)
            ]

        if is_model_type(value):
            return ModelType.from_model(value)

        if type_ is not None:
            dynamic = self._build(value, None)
            return self._build_from_dynamic(dynamic, type_)

        return type(value)

    def _build_from_iterable(self, values: Iterable, type_: Optional[type]) -> type:
        values = tuple(values)
        if len(values) == 0:
            return type_

        options = tuple(self._build(value, type_) for value in values)
        return build_union(options)

    @staticmethod
    def _build_from_dynamic(dynamic: type, static: Optional[type]) -> type:
        return dynamic if not len(get_args(static)) and TypeHintComparator(dynamic, static).match() else static


class TypeHintParser:
    """Type Hint Parser class."""

    def __init__(self, type_: Optional[type] = None):
        self.type_ = type_

    def build(self) -> type:
        """Parse type hint.

        :return: A type.
        """
        return self._build(self.type_)

    @classmethod
    @lru_cache()
    def _build(cls, type_: Optional[type]) -> type:
        if is_model_subclass(type_):
            # noinspection PyTypeChecker
            return ModelType.from_model(type_)

        origin = get_origin(type_)
        if origin is None:
            return type_
        args = get_args(type_)
        return cls._build(origin)[tuple(cls._build(arg) for arg in args)]
