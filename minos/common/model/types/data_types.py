"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from dataclasses import (
    dataclass,
    field,
)
from typing import (
    Any,
    Generic,
    Optional,
    TypeVar,
)

T = TypeVar("T")
NoneType = type(None)


class MissingSentinel(Generic[T]):
    """Class to detect when a field is not initialized."""


@dataclass
class Fixed(Generic[T]):
    """
    Represents an Avro Fixed type
    size (int): Specifying the number of bytes per value
    """

    size: int
    default: Any = field(default=MissingSentinel)
    namespace: Optional[str] = None
    aliases: Optional[list[Any]] = None
    _dataclasses_custom_type: str = "Fixed"

    def __repr__(self) -> str:
        return f"Fixed(size={self.size})"


@dataclass
class Enum(Generic[T]):
    """
    Represents an Avro Enum type
    symbols (typing.List): Specifying the possible values for the enum
    """

    symbols: list[Any]
    default: Any = field(default=MissingSentinel)
    namespace: Optional[str] = None
    aliases: Optional[list[Any]] = None
    docs: Optional[str] = None
    _dataclasses_custom_type: str = "Enum"

    def __repr__(self) -> str:
        return f"Enum(symbols={self.symbols})"


@dataclass
class Decimal(Generic[T]):
    """
    Represents an Avro Decimal type
    precision (int): Specifying the number precision
    scale(int): Specifying the number scale. Default 0
    """

    precision: int
    scale: int = 0
    default: Any = field(default=MissingSentinel)
    _dataclasses_custom_type: str = "Decimal"

    # Decimal serializes to bytes, which doesn't support namespace
    aliases: Optional[list[Any]] = None

    def __repr__(self) -> str:
        return f"Decimal(precision={self.precision}, scale={self.scale})"


@dataclass
class ModelRef(Generic[T]):
    """Represents an Avro Model Reference type."""

    default: Any = field(default=MissingSentinel)
    namespace: Optional[str] = None
    aliases: Optional[list[Any]] = None
    _dataclasses_custom_type: str = "ModelRef"

    def __repr__(self) -> str:
        return "ModelRef()"
