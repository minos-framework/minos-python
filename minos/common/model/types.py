"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import dataclasses
import datetime
import typing as t
import uuid

T = t.TypeVar("T")


class MissingSentinel(t.Generic[T]):
    """
    Class to detect when a field is not initialized
    """

    pass


@dataclasses.dataclass
class Fixed(t.Generic[T]):
    """
    Represents an Avro Fixed type
    size (int): Specifying the number of bytes per value
    """

    size: int
    default: t.Any = dataclasses.field(default=MissingSentinel)
    namespace: t.Optional[str] = None
    aliases: t.Optional[list[t.Any]] = None
    _dataclasses_custom_type: str = "Fixed"

    def __repr__(self) -> str:
        return f"Fixed(size={self.size})"


@dataclasses.dataclass
class Enum(t.Generic[T]):
    """
    Represents an Avro Enum type
    symbols (typing.List): Specifying the possible values for the enum
    """

    symbols: list[t.Any]
    default: t.Any = dataclasses.field(default=MissingSentinel)
    namespace: t.Optional[str] = None
    aliases: t.Optional[list[t.Any]] = None
    docs: t.Optional[str] = None
    _dataclasses_custom_type: str = "Enum"

    def __repr__(self) -> str:
        return f"Enum(symbols={self.symbols})"


@dataclasses.dataclass
class Decimal(t.Generic[T]):
    """
    Represents an Avro Decimal type
    precision (int): Specifying the number precision
    scale(int): Specifying the number scale. Default 0
    """

    precision: int
    scale: int = 0
    default: t.Any = dataclasses.field(default=MissingSentinel)
    _dataclasses_custom_type: str = "Decimal"

    # Decimal serializes to bytes, which doesn't support namespace
    aliases: t.Optional[list[t.Any]] = None

    def __repr__(self) -> str:
        return f"Decimal(precision={self.precision}, scale={self.scale})"


@dataclasses.dataclass
class ModelRef(t.Generic[T]):
    """Represents an Avro Model Reference type."""

    default: t.Any = dataclasses.field(default=MissingSentinel)
    namespace: t.Optional[str] = None
    aliases: t.Optional[list[t.Any]] = None
    _dataclasses_custom_type: str = "ModelRef"

    def __repr__(self) -> str:
        return "ModelRef()"


class ModelType(type):
    """Model Type class."""

    name: str
    namespace: str
    type_hints: dict[str, t.Type[T]]

    @classmethod
    def build(mcs, name: str, type_hints: dict[str, type], namespace: t.Optional[str] = str()) -> t.Type[T]:
        """Build a new ``ModelType`` instance.

        :param name: Name of the new type.
        :param type_hints: Type hints of the new type.
        :param namespace: Namespace of the new type.
        :return: A ``ModelType`` instance.
        """
        # noinspection PyTypeChecker
        return mcs(name, tuple(), {"type_hints": type_hints, "namespace": namespace})

    @classmethod
    def from_typed_dict(mcs, typed_dict) -> t.Type[T]:
        """Build a new ``ModelType`` instance from a ``typing.TypedDict``.

        :param typed_dict: Typed dict to be used as base.
        :return: A ``ModelType`` instance.
        """
        try:
            namespace, name = typed_dict.__name__.rsplit(".", 1)
        except ValueError:
            namespace, name = str(), typed_dict.__name__
        return mcs.build(name, typed_dict.__annotations__, namespace)

    @property
    def name(cls) -> str:
        """Get the type name.

        :return: A string object.
        """
        return cls.__name__

    @property
    def classname(cls) -> str:
        """Get the full class name.

        :return: An string object.
        """
        if len(cls.namespace) == 0:
            return cls.name
        return f"{cls.namespace}.{cls.name}"

    def __eq__(self, other) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> t.Iterable:
        # noinspection PyRedundantParentheses
        yield from (self.name, self.namespace, tuple(self.type_hints.items()))

    def __repr__(self):
        return (
            f"{type(self).__name__}(name={self.name!r}, namespace={self.namespace!r}, type_hints={self.type_hints!r})"
        )


BOOLEAN = "boolean"
NULL = "null"
INT = "int"
FLOAT = "float"
LONG = "long"
DOUBLE = "double"
BYTES = "bytes"
STRING = "string"
ARRAY = "array"
ENUM = "enum"
MAP = "map"
FIXED = "fixed"
DATE = "date"
TIME_MILLIS = "time-millis"
TIMESTAMP_MILLIS = "timestamp-millis"
UUID = "uuid"
DECIMAL = "decimal"

DATE_TYPE = {"type": INT, "logicalType": DATE}
TIME_TYPE = {"type": INT, "logicalType": TIME_MILLIS}
DATETIME_TYPE = {"type": LONG, "logicalType": TIMESTAMP_MILLIS}
UUID_TYPE = {"type": STRING, "logicalType": UUID}

PYTHON_TYPE_TO_AVRO = {
    bool: BOOLEAN,
    type(None): NULL,
    int: LONG,
    float: DOUBLE,
    bytes: BYTES,
    str: STRING,
    list: ARRAY,
    tuple: ARRAY,
    dict: MAP,
    Fixed: {"type": FIXED},
    Enum: {"type": ENUM},
    datetime.date: DATE_TYPE,
    datetime.time: TIME_TYPE,
    datetime.datetime: DATETIME_TYPE,
    uuid.uuid4: UUID_TYPE,
}

PYTHON_IMMUTABLE_TYPES = (str, int, bool, float, bytes)
PYTHON_IMMUTABLE_TYPES_STR = (STRING, INT, BOOLEAN, FLOAT, BYTES)
PYTHON_LIST_TYPES = (list, tuple)
PYTHON_ARRAY_TYPES = (dict,)
PYTHON_NULL_TYPE = type(None)
CUSTOM_TYPES = (
    "Fixed",
    "Enum",
    "Decimal",
    "ModelRef",
)
