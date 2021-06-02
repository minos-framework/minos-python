"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
import typing as t
from base64 import (
    b64decode,
    b64encode,
)
from itertools import (
    zip_longest,
)

from ...exceptions import (
    EmptyMinosModelSequenceException,
    MinosModelException,
    MultiTypeMinosModelSequenceException,
)
from ...importlib import (
    classname,
)
from ...meta import (
    classproperty,
    property_or_classproperty,
    self_or_classmethod,
)
from ...protocol import (
    MinosAvroProtocol,
)
from .fields import (
    ModelField,
    _MinosModelAvroSchemaBuilder,
)
from .types import (
    MissingSentinel,
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


class MinosModel(object):
    """Base class for ``minos`` model entities."""

    _fields: dict[str, ModelField] = {}

    def __init__(self, *args, **kwargs):
        """Class constructor.

        :param kwargs: Named arguments to be set as model attributes.
        """
        self._fields = dict()
        self._list_fields(*args, **kwargs)

    @classmethod
    def from_avro_str(cls, raw: str, **kwargs) -> t.Union[MinosModel, list[MinosModel]]:
        """Build a single instance or a sequence of instances from bytes

        :param raw: A bytes data.
        :return: A single instance or a sequence of instances.
        """
        raw = b64decode(raw.encode())
        return cls.from_avro_bytes(raw, **kwargs)

    @classmethod
    def from_avro_bytes(cls, raw: bytes, **kwargs) -> t.Union[MinosModel, list[MinosModel]]:
        """Build a single instance or a sequence of instances from bytes

        :param raw: A bytes data.
        :return: A single instance or a sequence of instances.
        """

        decoded = MinosAvroProtocol().decode(raw)
        if isinstance(decoded, list):
            return [cls.from_dict(d | kwargs) for d in decoded]
        return cls.from_dict(decoded | kwargs)

    @classmethod
    def from_dict(cls, d: dict[str, t.Any]) -> MinosModel:
        """Build a new instance from a dictionary.

        :param d: A dictionary object.
        :return: A new ``MinosModel`` instance.
        """
        return cls(**d)

    @classmethod
    def to_avro_str(cls, models: list[MinosModel]) -> str:
        """Create a bytes representation of the given object instances.

        :param models: A sequence of minos models.
        :return: A bytes object.
        """
        return b64encode(cls.to_avro_bytes(models)).decode()

    @classmethod
    def to_avro_bytes(cls, models: list[MinosModel]) -> bytes:
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
    @classproperty
    def classname(cls) -> str:
        """Compute the current class namespace.

        :return: An string object.
        """
        # noinspection PyTypeChecker
        return classname(cls)

    @property
    def fields(self) -> dict[str, ModelField]:
        """Fields getter"""
        return self._fields

    def __setattr__(self, key: str, value: t.Any) -> t.NoReturn:
        if self._fields is not None and key in self._fields:
            field_class: ModelField = self._fields[key]
            field_class.value = value
            self._fields[key] = field_class
        elif key.startswith("_"):
            super().__setattr__(key, value)
        else:
            raise MinosModelException(f"model attribute {key} doesn't exist")

    def __getattr__(self, item: str) -> t.Any:
        if self._fields is not None and item in self._fields:
            return self._fields[item].value
        else:
            raise AttributeError(f"{type(self).__name__!r} does not contain the {item!r} attribute.")

    def _list_fields(self, *args, **kwargs) -> t.NoReturn:
        for (name, type_val), value in zip_longest(self._type_hints(), args, fillvalue=MissingSentinel):
            if name in kwargs and value is not MissingSentinel:
                raise TypeError(f"got multiple values for argument {repr(name)}")

            if value is MissingSentinel and name in kwargs:
                value = kwargs[name]

            self._fields[name] = ModelField(
                name, type_val, value, getattr(self, f"parse_{name}", None), getattr(self, f"validate_{name}", None)
            )

    # noinspection PyMethodParameters
    @self_or_classmethod
    def _type_hints(self_or_cls) -> dict[str, t.Any]:
        fields = dict()
        if isinstance(self_or_cls, type):
            cls = self_or_cls
        else:
            cls = type(self_or_cls)
        for b in cls.__mro__[::-1]:
            base_fields = getattr(b, "_fields", None)
            if base_fields is not None:
                list_fields = {k: v for k, v in t.get_type_hints(b).items() if not k.startswith("_")}
                fields |= list_fields
        logger.debug(f"The obtained fields are: {fields!r}")
        yield from fields.items()

    # noinspection PyMethodParameters
    @property_or_classproperty
    def avro_schema(self_or_cls) -> list[dict[str, t.Any]]:
        """Compute the avro schema of the model.

        :return: A dictionary object.
        """
        if isinstance(self_or_cls, type):
            cls = self_or_cls
        else:
            cls = type(self_or_cls)

        fields = [
            _MinosModelAvroSchemaBuilder(field_name, field_type).build()
            for field_name, field_type in self_or_cls._type_hints()
        ]
        return [{"name": cls.__name__, "namespace": cls.__module__, "type": "record", "fields": fields}]

    @property
    def avro_data(self) -> dict[str, t.Any]:
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

    def __eq__(self, other: MinosModel) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> t.Iterable:
        # noinspection PyRedundantParentheses
        yield from self.fields.items()

    def __repr__(self):
        fields_repr = ", ".join(repr(field) for field in self.fields.values())
        return f"{type(self).__name__}(fields=[{fields_repr}])"
