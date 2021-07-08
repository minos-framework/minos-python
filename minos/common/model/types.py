"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import dataclasses
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterable,
    Optional,
    Type,
    TypeVar,
    Union,
)

from ..exceptions import (
    MinosImportException,
)
from ..importlib import (
    import_module,
)

if TYPE_CHECKING:
    from .abc import (
        Model,
    )

T = TypeVar("T")

NoneType = type(None)


class MissingSentinel(Generic[T]):
    """Class to detect when a field is not initialized."""


@dataclasses.dataclass
class Fixed(Generic[T]):
    """
    Represents an Avro Fixed type
    size (int): Specifying the number of bytes per value
    """

    size: int
    default: Any = dataclasses.field(default=MissingSentinel)
    namespace: Optional[str] = None
    aliases: Optional[list[Any]] = None
    _dataclasses_custom_type: str = "Fixed"

    def __repr__(self) -> str:
        return f"Fixed(size={self.size})"


@dataclasses.dataclass
class Enum(Generic[T]):
    """
    Represents an Avro Enum type
    symbols (typing.List): Specifying the possible values for the enum
    """

    symbols: list[Any]
    default: Any = dataclasses.field(default=MissingSentinel)
    namespace: Optional[str] = None
    aliases: Optional[list[Any]] = None
    docs: Optional[str] = None
    _dataclasses_custom_type: str = "Enum"

    def __repr__(self) -> str:
        return f"Enum(symbols={self.symbols})"


@dataclasses.dataclass
class Decimal(Generic[T]):
    """
    Represents an Avro Decimal type
    precision (int): Specifying the number precision
    scale(int): Specifying the number scale. Default 0
    """

    precision: int
    scale: int = 0
    default: Any = dataclasses.field(default=MissingSentinel)
    _dataclasses_custom_type: str = "Decimal"

    # Decimal serializes to bytes, which doesn't support namespace
    aliases: Optional[list[Any]] = None

    def __repr__(self) -> str:
        return f"Decimal(precision={self.precision}, scale={self.scale})"


@dataclasses.dataclass
class ModelRef(Generic[T]):
    """Represents an Avro Model Reference type."""

    default: Any = dataclasses.field(default=MissingSentinel)
    namespace: Optional[str] = None
    aliases: Optional[list[Any]] = None
    _dataclasses_custom_type: str = "ModelRef"

    def __repr__(self) -> str:
        return "ModelRef()"


class ModelType(type):
    """Model Type class."""

    name: str
    namespace: str
    type_hints: dict[str, Type[T]]

    @classmethod
    def build(mcs, name: str, type_hints: dict[str, type], namespace: Optional[str] = None) -> Type[T]:
        """Build a new ``ModelType`` instance.

        :param name: Name of the new type.
        :param type_hints: Type hints of the new type.
        :param namespace: Namespace of the new type.
        :return: A ``ModelType`` instance.
        """
        if namespace is None:
            try:
                namespace, name = name.rsplit(".", 1)
            except ValueError:
                namespace = str()

        # noinspection PyTypeChecker
        return mcs(name, tuple(), {"type_hints": type_hints, "namespace": namespace})

    @classmethod
    def from_typed_dict(mcs, typed_dict) -> Type[T]:
        """Build a new ``ModelType`` instance from a ``typing.TypedDict``.

        :param typed_dict: Typed dict to be used as base.
        :return: A ``ModelType`` instance.
        """
        return mcs.build(typed_dict.__name__, typed_dict.__annotations__)

    def __call__(cls, *args, **kwargs) -> Model:
        return cls.model_cls.from_model_type(cls, kwargs)

    @property
    def model_cls(cls) -> Type[Model]:
        """Get the model class if defined or ``DataTransferObject`` otherwise.

        :return: A model class.
        """
        try:
            # noinspection PyTypeChecker
            return import_module(cls.classname)
        except MinosImportException:
            from .dynamic import (
                DataTransferObject,
            )

            return DataTransferObject

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

    def __eq__(cls, other: Union[ModelType, Type[Model]]) -> bool:
        conditions = (
            cls._equal_with_model_type,
            cls._equal_with_model,
            cls._equal_with_inherited_model,
            cls._equal_with_bucket_model,
        )
        # noinspection PyArgumentList
        return any(condition(other) for condition in conditions)

    def _equal_with_model_type(cls, other: ModelType) -> bool:
        from .utils import (
            TypeHintComparator,
        )

        return (
            type(cls) == type(other)
            and cls.name == other.name
            and cls.namespace == other.namespace
            and set(cls.type_hints.keys()) == set(other.type_hints.keys())
            and all(TypeHintComparator(v, other.type_hints[k]).match() for k, v in cls.type_hints.items())
        )

    def _equal_with_model(cls, other: Any) -> bool:
        return hasattr(other, "model_type") and cls == other.model_type

    def _equal_with_inherited_model(cls, other: ModelType) -> bool:
        return (
            type(cls) == type(other) and cls.model_cls != other.model_cls and issubclass(cls.model_cls, other.model_cls)
        )

    @staticmethod
    def _equal_with_bucket_model(other: Any) -> bool:
        from .dynamic import (
            BucketModel,
        )

        return hasattr(other, "model_cls") and issubclass(other.model_cls, BucketModel)

    def __hash__(cls) -> int:
        return hash(tuple(cls))

    def __iter__(cls) -> Iterable:
        # noinspection PyRedundantParentheses
        yield from (cls.name, cls.namespace, tuple(cls.type_hints.items()))

    def __repr__(cls):
        return f"{type(cls).__name__}(name={cls.name!r}, namespace={cls.namespace!r}, type_hints={cls.type_hints!r})"


def is_model_subclass(type_field: Any) -> bool:
    """Check if the given type field is subclass of ``Model``."""
    from .abc import (
        Model,
    )

    return issubclass(type_field, Model)


def is_aggregate_subclass(type_field: Any) -> bool:
    """Check if the given type field is subclass of ``Aggregate``."""
    from .declarative import (
        Aggregate,
    )

    return issubclass(type_field, Aggregate)


def is_type_subclass(type_field: Any) -> bool:
    """Check if the given type field is subclass of ``type``."""
    return issubclass(type(type_field), type(type))
