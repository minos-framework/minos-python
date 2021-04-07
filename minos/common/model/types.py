import dataclasses
import typing as t

T = t.TypeVar("T")


class MissingSentinel(t.Generic[T]):
    """
    Class to detect when a field is not initialized
    """

    ...


@dataclasses.dataclass
class Fixed(t.Generic[T]):
    """
    Represents an Avro Fixed type
    size (int): Specifying the number of bytes per value
    """

    size: int
    default: t.Any = dataclasses.field(default=MissingSentinel)
    namespace: t.Optional[str] = None
    aliases: t.Optional[t.List] = None
    _dataclasses_custom_type: str = "Fixed"

    def __repr__(self) -> str:
        return f"{self.size}"


@dataclasses.dataclass
class Enum(t.Generic[T]):
    """
    Represents an Avro Enum type
    simbols (typing.List): Specifying the possible values for the enum
    """

    symbols: t.List[t.Any]
    default: t.Any = dataclasses.field(default=MissingSentinel)
    namespace: t.Optional[str] = None
    aliases: t.Optional[t.List] = None
    docs: t.Optional[str] = None
    _dataclasses_custom_type: str = "Enum"

    def __repr__(self) -> str:
        return f"{self.symbols}"


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
    aliases: t.Optional[t.List] = None

    def __repr__(self) -> str:
        return f"Decimal precision: {self.precision} scale:{self.scale}"


CUSTOM_TYPES = ("Fixed", "Enum", "Decimal")
