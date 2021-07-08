"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from base64 import (
    b64decode,
    b64encode,
)
from typing import (
    Any,
    Generic,
    Iterable,
    NoReturn,
    Type,
    TypeVar,
    Union,
)

from ..exceptions import (
    EmptyMinosModelSequenceException,
    MinosModelException,
    MultiTypeMinosModelSequenceException,
)
from ..importlib import (
    classname,
)
from ..meta import (
    classproperty,
    property_or_classproperty,
    self_or_classmethod,
)
from ..protocol import (
    MinosAvroProtocol,
)
from .fields import (
    Field,
)
from .serializers import (
    AvroDataDecoder,
    AvroSchemaDecoder,
    AvroSchemaEncoder,
)
from .types import (
    ModelType,
)

logger = logging.getLogger(__name__)

# def _process_aggregate(cls):
#     """
#     Get the list of the class arguments and define it as an AggregateField class
#     """
#     cls_annotations = cls.__dict__.get('__annotations__', {})
#     aggregate_fields = []
#     for name, type in cls_annotations.items():
#         attribute = getattr(cls, name, None)
#         aggregate_fields.append(
#             AggregateField(name=name, type=type, value=attribute)
#         )
#     setattr(cls, "_FIELDS", aggregate_fields)
#
#     # g get metaclass
#     meta_class = getattr(cls, "Meta", None)
#     if meta_class:
#         # meta class exist so get the information related
#         ...
#     return cls
#
#
# def aggregate(cls=None):
#     def wrap(cls):
#         return _process_aggregate(cls)
#
#     if cls is None:
#         return wrap
#
#     return wrap(cls)

T = TypeVar("T")


class Model(Generic[T]):
    """Base class for ``minos`` model entities."""

    _fields: dict[str, Field] = {}

    def __init__(self, fields: Union[Iterable[Field], dict[str, Field]] = None):
        """Class constructor.

        :param fields: Dictionary that contains the ``Field`` instances of the model indexed by name.
        """
        if fields is None:
            fields = dict()
        if not isinstance(fields, dict):
            fields = {field.name: field for field in fields}
        self._fields = fields

    @classmethod
    def from_avro_str(cls, raw: str, **kwargs) -> Union[T, list[T]]:
        """Build a single instance or a sequence of instances from bytes

        :param raw: A bytes data.
        :return: A single instance or a sequence of instances.
        """
        raw = b64decode(raw.encode())
        return cls.from_avro_bytes(raw, **kwargs)

    @classmethod
    def from_avro_bytes(cls, raw: bytes, **kwargs) -> Union[T, list[T]]:
        """Build a single instance or a sequence of instances from bytes

        :param raw: A bytes data.
        :return: A single instance or a sequence of instances.
        """
        schema = MinosAvroProtocol.decode_schema(raw)
        decoded = MinosAvroProtocol.decode(raw)

        # FIXME: Extend implementation of the `AvroDataDecoder` to avoid this fix.
        schema["name"] = "{}.fake.{}".format(*schema["name"].rsplit(".", 1))

        if isinstance(decoded, list):
            return [cls.from_avro(schema, d | kwargs) for d in decoded]
        return cls.from_avro(schema, decoded | kwargs)

    @classmethod
    def from_avro(cls, schema: Union[dict[str, Any], list[dict[str, Any]]], data: dict[str, Any]) -> T:
        """Build a new instance from the ``avro`` schema and data.

        :param schema: The avro schema of the model.
        :param data: The avro data of the model.
        :return: A new ``DynamicModel`` instance.
        """
        if isinstance(schema, list):
            schema = schema[-1]
        model_type: ModelType = AvroSchemaDecoder(schema).build()
        return AvroDataDecoder("", model_type).build(data)

    @classmethod
    def to_avro_str(cls, models: list[T]) -> str:
        """Create a bytes representation of the given object instances.

        :param models: A sequence of minos models.
        :return: A bytes object.
        """
        return b64encode(cls.to_avro_bytes(models)).decode()

    @classmethod
    def to_avro_bytes(cls, models: list[T]) -> bytes:
        """Create a bytes representation of the given object instances.

        :param models: A sequence of minos models.
        :return: A bytes object.
        """
        if len(models) == 0:
            raise EmptyMinosModelSequenceException("'models' parameter cannot be empty.")

        model_type = type(models[0])
        if not all(model_type == type(model) for model in models):
            raise MultiTypeMinosModelSequenceException(
                f"Every model must have type {model_type} to be valid. Found types: {[type(model) for model in models]}"
            )

        avro_schema = models[0].avro_schema
        # noinspection PyTypeChecker
        return MinosAvroProtocol().encode([model.avro_data for model in models], avro_schema)

    # noinspection PyMethodParameters
    @property_or_classproperty
    def model_type(self_or_cls) -> Type[T]:
        """Get the model type of the instance.

        :return: A ``ModelType`` instance.
        """
        # noinspection PyTypeChecker
        return ModelType.build(self_or_cls.classname, self_or_cls.type_hints)

    # noinspection PyMethodParameters
    @classproperty
    def classname(cls) -> str:
        """Compute the current class namespace.
        :return: An string object.
        """
        # noinspection PyTypeChecker
        return classname(cls)

    @property
    def fields(self) -> dict[str, Field]:
        """Fields getter"""
        return self._fields

    def __setattr__(self, key: str, value: Any) -> NoReturn:
        if self._fields is not None and key in self._fields:
            field_class: Field = self._fields[key]
            field_class.value = value
            self._fields[key] = field_class
        elif key.startswith("_"):
            super().__setattr__(key, value)
        else:
            raise MinosModelException(f"model attribute {key} doesn't exist")

    def __getattr__(self, item: str) -> Any:
        if self._fields is not None and item in self._fields:
            return self._fields[item].value
        else:
            raise AttributeError(f"{type(self).__name__!r} does not contain the {item!r} attribute.")

    # noinspection PyMethodParameters
    @property_or_classproperty
    def type_hints(self_or_cls) -> dict[str, type]:
        """Get the type hinting of the instance or class.

        :return: A dictionary in which the keys are the field names and the values are the types.
        """
        return dict(self_or_cls._type_hints())

    # noinspection PyMethodParameters
    @self_or_classmethod
    def _type_hints(self_or_cls) -> dict[str, Any]:
        if not isinstance(self_or_cls, type):
            yield from ((field.name, field.real_type) for field in self_or_cls.fields.values())

    # noinspection PyMethodParameters
    @property_or_classproperty
    def avro_schema(self_or_cls) -> list[dict[str, Any]]:
        """Compute the avro schema of the model.

        :return: A dictionary object.
        """
        # noinspection PyTypeChecker
        return [AvroSchemaEncoder("", self_or_cls.model_type).build()["type"]]

    @property
    def avro_data(self) -> dict[str, Any]:
        """Compute the avro data of the model.

        :return: A dictionary object.
        """
        return {name: field.avro_data for name, field in self.fields.items()}

    @property
    def avro_str(self) -> str:
        """Generate bytes representation of the current instance.

        :return: A bytes object.
        """
        # noinspection PyTypeChecker
        return b64encode(self.avro_bytes).decode()

    @property
    def avro_bytes(self) -> bytes:
        """Generate bytes representation of the current instance.

        :return: A bytes object.
        """
        # noinspection PyTypeChecker
        return MinosAvroProtocol().encode(self.avro_data, self.avro_schema)

    def __eq__(self, other: T) -> bool:
        return type(self) == type(other) and self.fields == other.fields

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> Iterable:
        # noinspection PyRedundantParentheses
        yield from self.fields.values()

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)

    def __setitem__(self, key: str, value: Any) -> NoReturn:
        setattr(self, key, value)

    def keys(self) -> Iterable[str]:
        """Get the field names.

        :return: An iterable of string values.
        """
        return self.fields.keys()

    def __repr__(self) -> str:
        fields_repr = ", ".join(repr(field) for field in self.fields.values())
        return f"{type(self).__name__}({fields_repr})"
