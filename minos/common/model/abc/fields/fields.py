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
import typing as t

from minos.common.exceptions import (
    MinosAttributeValidationException,
    MinosParseAttributeException,
)
from minos.common.model.abc.types import (
    MissingSentinel,
)

from .avro_data_builder import (
    MinosModelAvroDataBuilder,
)
from .caster import (
    ModelFieldCaster,
)
from .model_builder import (
    MinosModelFromAvroBuilder,
)
from .schema_builder import (
    MinosModelAvroSchemaBuilder,
)

logger = logging.getLogger(__name__)

T = t.TypeVar("T")


class ModelField:
    """Represents a model field."""

    __slots__ = "_name", "_type", "_value", "_parser", "_validator"

    def __init__(
        self,
        name: str,
        type_val: t.Type[T],
        value: T = MissingSentinel,
        parser: t.Optional[t.Callable[[t.Any], T]] = None,
        validator: t.Optional[t.Callable[[t.Any], bool]] = None,
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
    def type(self) -> t.Type:
        """Type getter."""
        return self._type

    @property
    def parser(self) -> t.Optional[t.Callable[[t.Any], T]]:
        """Parser getter."""
        return self._parser

    @property
    def _parser_name(self) -> t.Optional[str]:
        if self.parser is None:
            return None
        return self.parser.__name__

    @property
    def _parser_function(self) -> t.Optional[t.Callable[[t.Any], T]]:
        if self.parser is None:
            return None
        if inspect.ismethod(self.parser):
            # noinspection PyUnresolvedReferences
            return self.parser.__func__
        return self.parser

    @property
    def validator(self) -> t.Optional[t.Callable[[t.Any], T]]:
        """Parser getter."""
        return self._validator

    @property
    def _validator_name(self) -> t.Optional[str]:
        if self.validator is None:
            return None
        return self.validator.__name__

    @property
    def _validator_function(self) -> t.Optional[t.Callable[[t.Any], T]]:
        if self.validator is None:
            return None
        if inspect.ismethod(self.validator):
            # noinspection PyUnresolvedReferences
            return self.validator.__func__

    @property
    def value(self) -> t.Any:
        """Value getter."""
        return self._value

    @value.setter
    def value(self, data: t.Any) -> t.NoReturn:
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

        value = ModelFieldCaster.from_field(self).cast(data)

        if self.validator is not None and value is not None and not self.validator(value):
            raise MinosAttributeValidationException(self.name, value)

        self._value = value

    @property
    def avro_schema(self) -> dict[str, t.Any]:
        """Compute the avro schema of the field.

        :return: A dictionary object.
        """
        return MinosModelAvroSchemaBuilder.from_field(self).build()

    @property
    def avro_data(self):
        """Compute the avro data of the model.

        :return: A dictionary object.
        """
        return MinosModelAvroDataBuilder.from_field(self).build()

    @classmethod
    def from_avro(cls, schema: dict, value: t.Any) -> ModelField:
        """Build a ``ModelField`` instance from the avro information.

        :param schema: Field's schema.
        :param value: Field's value.
        :return: A ``ModelField`` instance.
        """
        type_val = MinosModelFromAvroBuilder(schema).build()
        return cls(schema["name"], type_val, value)

    def __eq__(self, other: "ModelField") -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> t.Iterable:
        # noinspection PyRedundantParentheses
        yield from (self.name, self.type, self.value, self._parser_function, self._validator_function)

    def __repr__(self):
        return (
            f"ModelField(name={repr(self.name)}, type={repr(self.type)}, value={repr(self.value)}, "
            f"parser={self._parser_name}, validator={self._validator_name})"
        )
