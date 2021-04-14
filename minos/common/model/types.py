"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import dataclasses
import typing as t

T = t.TypeVar("T")


class MissingSentinel(t.Generic[T]):
    """
    Class to detect when a field is not initialized
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls, *args, **kwargs)
        return cls._instance

    def __eq__(self, other: t.Optional["MissingSentinel"]):
        return type(self) == type(other) or other is None

    def __hash__(self) -> int:
        return id(self)


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
    simbols (typing.List): Specifying the possible values for the enum
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


CUSTOM_TYPES = ("Fixed", "Enum", "Decimal", "ModelRef",)
