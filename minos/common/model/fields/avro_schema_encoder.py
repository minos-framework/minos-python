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
from datetime import (
    date,
    datetime,
    time,
)
from uuid import (
    UUID,
)

from ..types import (
    BOOLEAN,
    BYTES,
    DATE_TYPE,
    DATETIME_TYPE,
    DOUBLE,
    INT,
    NULL,
    PYTHON_IMMUTABLE_TYPES,
    STRING,
    TIME_TYPE,
    UUID_TYPE,
    ModelRef,
    ModelType,
)
from .utils import (
    _is_model_cls,
)

if t.TYPE_CHECKING:
    from .fields import ModelField  # pragma: no cover

logger = logging.getLogger(__name__)

T = t.TypeVar("T")


class AvroSchemaEncoder:
    """Avro Schema Encoder class."""

    def __init__(self, field_name: str, field_type: t.Type):
        self._name = field_name
        self._type = field_type

    @classmethod
    def from_field(cls, field: ModelField) -> AvroSchemaEncoder:
        """Build a new instance from a ``ModelField``.

        :param field: The model field.
        :return: A new avro schema builder instance.
        """
        return cls(field.name, field.type)

    def build(self) -> dict[str, t.Any]:
        """Build the avro schema for the given field.

        :return: A dictionary object.
        """

        return {"name": self._name, "type": self._build_schema(self._type)}

    def _build_schema(self, type_field: t.Type) -> t.Any:
        origin = t.get_origin(type_field)
        if origin is not t.Union:
            return self._build_single_schema(type_field)
        return self._build_union_schema(type_field)

    def _build_union_schema(self, type_field: t.Type) -> t.Any:
        ans = list()
        alternatives = t.get_args(type_field)
        for alternative_type in alternatives:
            step = self._build_single_schema(alternative_type)
            if isinstance(step, list):
                ans += step
            else:
                ans.append(step)
        return ans

    def _build_single_schema(self, type_field: t.Type) -> t.Any:
        if type_field is type(None):  # noqa: E721
            return self._build_none_schema(type_field)

        if type_field in PYTHON_IMMUTABLE_TYPES:
            return self._build_simple_schema(type_field)

        if type_field is date:
            return DATE_TYPE

        if type_field is time:
            return TIME_TYPE

        if type_field is datetime:
            return DATETIME_TYPE

        if type_field is UUID:
            return UUID_TYPE

        if isinstance(type_field, ModelType):
            return self._build_model_type_schema(type_field)

        if _is_model_cls(type_field):
            return self._build_model_schema(type_field)

        return self._build_composed_schema(type_field)

    @staticmethod
    def _build_none_schema(type_field: t.Type) -> t.Any:
        if type_field is type(None):  # noqa: E721
            return NULL

        raise ValueError(f"Given field type is not supported: {type_field}")  # pragma: no cover

    @staticmethod
    def _build_simple_schema(type_field: t.Type) -> t.Any:
        if type_field is int:
            return INT

        if type_field is bool:
            return BOOLEAN

        if type_field is float:
            return DOUBLE

        if type_field is str:
            return STRING

        if type_field is bytes:
            return BYTES

        raise ValueError(f"Given field type is not supported: {type_field}")  # pragma: no cover

    def _build_model_type_schema(self, type_field: ModelType) -> t.Any:
        namespace = type_field.namespace
        if len(namespace) > 0 and len(self._name) > 0:
            namespace = f"{type_field.namespace}.{self._name}"
        schema = {
            "name": type_field.name,
            "namespace": namespace,
            "type": "record",
            "fields": [AvroSchemaEncoder(k, v).build() for k, v in type_field.type_hints.items()],
        }
        return schema

    def _build_model_schema(self, type_field: t.Type) -> t.Any:
        def _patch_namespace(s: dict) -> dict:
            if len(self._name) > 0:
                s["namespace"] += f".{self._name}"
            return s

        # noinspection PyUnresolvedReferences
        return [_patch_namespace(s) for s in type_field.avro_schema]

    def _build_composed_schema(self, type_field: t.Type) -> t.Any:
        origin_type = t.get_origin(type_field)

        if origin_type is list:
            return self._build_list_schema(type_field)

        if origin_type is dict:
            return self._build_dict_schema(type_field)

        if origin_type is ModelRef:
            return self._build_model_ref_schema(type_field)

        raise ValueError(f"Given field type is not supported: {type_field}")  # pragma: no cover

    def _build_list_schema(self, type_field: t.Type) -> dict[str, t.Any]:
        return {"type": "array", "items": self._build_schema(t.get_args(type_field)[0])}

    def _build_dict_schema(self, type_field: t.Type) -> dict[str, t.Any]:
        return {"type": "map", "values": self._build_schema(t.get_args(type_field)[1])}

    def _build_model_ref_schema(self, type_field: t.Type) -> t.Union[bool, t.Any]:
        return self._build_schema(t.Union[t.get_args(type_field)[0], int])
