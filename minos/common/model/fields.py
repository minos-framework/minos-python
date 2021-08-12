"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import inspect
import logging
from typing import (
    Any,
    Callable,
    Iterable,
    NoReturn,
    Optional,
    Type,
    TypeVar,
)

from ..exceptions import (
    MinosAttributeValidationException,
    MinosParseAttributeException,
)
from .serializers import (
    AvroDataDecoder,
    AvroDataEncoder,
    AvroSchemaDecoder,
    AvroSchemaEncoder,
)
from .types import (
    MissingSentinel,
    TypeHintBuilder,
    TypeHintComparator,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class Field:
    """Represents a model field."""

    __slots__ = "_name", "_type", "_value", "_parser", "_validator"

    def __init__(
        self,
        name: str,
        type_val: Type[T],
        value: T = MissingSentinel,
        parser: Optional[Callable[[Any], T]] = None,
        validator: Optional[Callable[[Any], bool]] = None,
    ):
        self._name = name
        self._type = type_val
        self._parser = parser
        self._validator = validator

        self.value = value

    @property
    def name(self) -> str:
        """Name getter."""
        return self._name

    @property
    def type(self) -> Type[T]:
        """Type getter."""
        return self._type

    @property
    def real_type(self):
        """Real Type getter."""
        return TypeHintBuilder(self.value, self.type).build()

    @property
    def parser(self) -> Optional[Callable[[Any], T]]:
        """Parser getter."""
        return self._parser

    @property
    def _parser_function(self) -> Optional[Callable[[Any], T]]:
        if self.parser is None:
            return None
        if inspect.ismethod(self.parser):
            # noinspection PyUnresolvedReferences
            return self.parser.__func__
        return self.parser

    @property
    def validator(self) -> Optional[Callable[[Any], T]]:
        """Parser getter."""
        return self._validator

    @property
    def _validator_function(self) -> Optional[Callable[[Any], T]]:
        if self.validator is None:
            return None
        if inspect.ismethod(self.validator):
            # noinspection PyUnresolvedReferences
            return self.validator.__func__

    @property
    def value(self) -> Any:
        """Value getter."""
        return self._value

    @value.setter
    def value(self, data: Any) -> NoReturn:
        """Check if the given value is correct and stores it if ``True``, otherwise raises an exception.

        :param data: new value.
        :return: This method does not return anything.
        """
        logger.debug(f"Setting {data!r} value to {self._name!r} field with {self._type!r} type...")

        if self._parser is not None:
            try:
                data = self.parser(data)
            except Exception as exc:
                raise MinosParseAttributeException(self.name, data, exc)

        value = AvroDataDecoder.from_field(self).build(data)

        if self.validator is not None and value is not None and not self.validator(value):
            raise MinosAttributeValidationException(self.name, value)

        self._value = value

    @property
    def avro_schema(self) -> dict[str, Any]:
        """Compute the avro schema of the field.

        :return: A dictionary object.
        """
        return AvroSchemaEncoder.from_field(self).build()

    @property
    def avro_data(self):
        """Compute the avro data of the model.

        :return: A dictionary object.
        """
        return AvroDataEncoder.from_field(self).build()

    @classmethod
    def from_avro(cls, schema: dict, value: Any) -> Field:
        """Build a ``Field`` instance from the avro information.

        :param schema: Field's schema.
        :param value: Field's value.
        :return: A ``Field`` instance.
        """
        type_val = AvroSchemaDecoder(schema).build()
        return cls(schema["name"], type_val, value)

    def __eq__(self, other: Field) -> bool:
        return (
            type(self) == type(other)
            and self.name == other.name
            and self.value == other.value
            and self._parser_function == other._parser_function
            and self._validator_function == other._validator_function
            and TypeHintComparator(self.type, other.type).match()
        )

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> Iterable:
        # noinspection PyRedundantParentheses
        yield from (self.name, self.type, self.value, self._parser_function, self._validator_function)

    def __repr__(self):
        return f"{self.name}={self.value!s}"


ModelField = Field
