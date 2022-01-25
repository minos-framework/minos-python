from __future__ import (
    annotations,
)

import logging
from contextlib import (
    suppress,
)
from datetime import (
    date,
    datetime,
    time,
    timedelta,
)
from typing import (
    Any,
    Union,
)
from uuid import (
    UUID,
)

from .....exceptions import (
    MinosImportException,
    MinosMalformedAttributeException,
)
from .....importlib import (
    import_module,
)
from ....types import (
    MissingSentinel,
    ModelType,
    NoneType,
    build_union,
    is_model_subclass,
)
from ...abc import (
    SchemaDecoder,
)
from .constants import (
    AVRO_ARRAY,
    AVRO_BOOLEAN,
    AVRO_BYTES,
    AVRO_DATE,
    AVRO_DOUBLE,
    AVRO_FLOAT,
    AVRO_INT,
    AVRO_MAP,
    AVRO_NULL,
    AVRO_RECORD,
    AVRO_SET,
    AVRO_STRING,
    AVRO_TIME,
    AVRO_TIMEDELTA,
    AVRO_TIMESTAMP,
    AVRO_UUID,
)

logger = logging.getLogger(__name__)


class AvroSchemaDecoder(SchemaDecoder):
    """Avro Schema Decoder class."""

    def __init__(self, schema: Any = None):
        self._schema = schema

    def build(self, schema: Any = MissingSentinel, **kwargs) -> type:
        """Build type from given avro schema item.

        :param schema: The schema to be decoded as a type.
        :return: A type object.
        """
        if schema is MissingSentinel:
            schema = self._schema
        return self._build(schema, **kwargs)

    def _build(self, schema: Union[dict, list, str], **kwargs) -> type:
        if isinstance(schema, dict):
            return self._build_from_dict(schema, **kwargs)

        if isinstance(schema, list):
            return self._build_from_list(schema, **kwargs)

        return self._build_simple(schema, **kwargs)

    def _build_from_list(self, schema: list[Any], **kwargs) -> type:
        options = list()
        for entry in schema:
            with suppress(Exception):
                options.append(self._build(entry, **kwargs))
        return build_union(options)

    def _build_from_dict(self, schema: dict, **kwargs) -> type:
        if "logicalType" in schema:
            return self._build_logical_type(schema, **kwargs)

        if schema["type"] == AVRO_ARRAY:
            return self._build_list(schema, **kwargs)

        if schema["type"] == AVRO_MAP:
            return self._build_dict(schema, **kwargs)

        if schema["type"] == AVRO_RECORD:
            return self._build_record(schema, **kwargs)

        return self._build_type(schema, **kwargs)

    def _build_logical_type(self, schema: dict[str, Any], **kwargs) -> type:
        logical_type = schema["logicalType"]
        if logical_type == AVRO_DATE["logicalType"]:
            return date

        if logical_type == AVRO_TIME["logicalType"]:
            return time

        if logical_type == AVRO_TIMESTAMP["logicalType"]:
            return datetime

        if logical_type == AVRO_TIMEDELTA["logicalType"]:
            return timedelta

        if logical_type == AVRO_UUID["logicalType"]:
            return UUID

        if logical_type == AVRO_SET["logicalType"]:
            return self._build_set(schema, **kwargs)

        sub_schema = {k: v for k, v in schema.items() if k != "logicalType"}

        try:
            cls_ = import_module(logical_type)
        except MinosImportException:
            cls_ = None

        if cls_ is not None:
            # noinspection PyUnresolvedReferences
            if is_model_subclass(cls_) and (ans := cls_.decode_schema(self, sub_schema)) is not MissingSentinel:
                return ans

            return cls_

        return self._build(sub_schema, **kwargs)

    def _build_list(self, schema: dict[str, Any], **kwargs) -> type:
        items = schema["items"]
        return list[self._build_iterable(items, **kwargs)]

    def _build_set(self, schema: dict[str, Any], **kwargs) -> type:
        items = schema["items"]
        return set[self._build_iterable(items, **kwargs)]

    def _build_dict(self, schema: dict[str, Any], **kwargs) -> type:
        values = schema["values"]
        return dict[str, self._build_iterable(values, **kwargs)]

    def _build_iterable(self, values: Union[dict, str, Any], **kwargs) -> type:
        type_ = self._build(values, **kwargs)
        if type_ is NoneType:
            # FIXME: This is a design decision that must be revisited in the future.
            type_ = Any
        return type_

    def _build_record(self, schema: dict[str, Any], **kwargs) -> type:
        name, namespace = schema["name"], schema.get("namespace")
        if namespace is None:
            try:
                namespace, name = name.rsplit(".", 1)
            except ValueError:
                namespace = str()

        namespace = self._unpatch_namespace(namespace)

        if len(namespace) > 0:
            classname = f"{namespace}.{name}"
        else:
            classname = name

        try:
            cls_ = import_module(classname)
        except MinosImportException:
            cls_ = None

        if is_model_subclass(cls_) and (ans := cls_.decode_schema(self, schema, **kwargs)) is not MissingSentinel:
            return ans

        type_hints = {field["name"]: self._build(field, **kwargs) for field in schema["fields"]}

        return ModelType.build(name_=name, type_hints_=type_hints, namespace_=namespace)

    @staticmethod
    def _unpatch_namespace(namespace: str) -> str:
        if len(namespace) > 0:
            return namespace.rsplit(".", 1)[0]
        return namespace

    def _build_type(self, schema: dict[str, Any], **kwargs) -> type:
        return self._build(schema["type"], **kwargs)

    @staticmethod
    def _build_simple(type_: str, **kwargs) -> type:
        if type_ == AVRO_NULL:
            return NoneType

        if type_ == AVRO_INT:
            return int

        if type_ == AVRO_BOOLEAN:
            return bool

        if type_ == AVRO_FLOAT:
            return float

        if type_ == AVRO_DOUBLE:
            return float

        if type_ == AVRO_STRING:
            return str

        if type_ == AVRO_BYTES:
            return bytes

        raise MinosMalformedAttributeException(f"Given field type is not supported: {type_!r}")
