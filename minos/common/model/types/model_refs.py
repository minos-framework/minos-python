"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from collections import (
    defaultdict,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterable,
    NoReturn,
    Optional,
    TypeVar,
    get_args,
    get_origin,
)
from uuid import (
    UUID,
)

from .comparators import (
    is_model_type,
)

if TYPE_CHECKING:
    from ..abc import (
        Model,
    )

T = TypeVar("T")


class ModelRef(Generic[T]):
    """Model Reference."""


class ModelRefExtractor:
    """Model Reference Extractor class."""

    def __init__(self, value: Any, type_: Optional[type] = None):
        if type_ is None:
            from .builders import (
                TypeHintBuilder,
            )

            type_ = TypeHintBuilder(value).build()
        self.value = value
        self.type_ = type_

    def build(self) -> dict[str, set[UUID]]:
        """Run the model reference extractor.

        :return: A dictionary in which the keys are the class names and the values are the identifiers.
        """
        ans = defaultdict(set)
        self._build(self.value, self.type_, ans)
        return ans

    def _build(self, value: Any, type_: type, ans: dict[str, set[UUID]]) -> NoReturn:
        if isinstance(value, (tuple, list, set)):
            self._build_iterable(value, get_args(type_)[0], ans)

        elif isinstance(value, dict):
            self._build_iterable(value.keys(), get_args(type_)[0], ans)
            self._build_iterable(value.values(), get_args(type_)[1], ans)

        elif is_model_type(value):
            for field in value.fields.values():
                self._build(field.value, field.type, ans)

        elif get_origin(type_) is ModelRef and isinstance(value, UUID):
            cls = get_args(type_)[0]
            name = cls.__name__
            ans[name].add(value)

    def _build_iterable(self, value: Iterable, value_: type, ans: dict[str, set[UUID]]) -> NoReturn:
        for sub_value in value:
            self._build(sub_value, value_, ans)


class ModelRefInjector:
    """Model Reference Injector class."""

    def __init__(self, value: Any, mapper: dict[UUID, Model]):
        self.value = value
        self.mapper = mapper

    def build(self) -> Any:
        """Inject the model instances referenced by identifiers.

        :return: A model in which the model references have been replaced by the values.
        """
        return self._build(self.value)

    def _build(self, value: Any) -> NoReturn:
        if isinstance(value, (tuple, list, set)):
            return type(value)(self._build(v) for v in value)

        if isinstance(value, dict):
            return type(value)((self._build(k), self._build(v)) for k, v in value.items())

        if is_model_type(value):
            for field in value.fields.values():
                field.value = self._build(field.value)
            return value

        if isinstance(value, UUID) and value in self.mapper:
            return self.mapper[value]

        return value
