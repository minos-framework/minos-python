"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from datetime import (
    date,
    datetime,
    time,
)
from typing import (
    Any,
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from ...exceptions import (
    MinosMalformedAttributeException,
)
from ..types import (
    ModelType,
    NoneType,
    build_union,
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
    AVRO_STRING,
    AVRO_TIME,
    AVRO_TIMESTAMP,
    AVRO_UUID,
)

logger = logging.getLogger(__name__)


class AvroSchemaDecoder:
    """Avro Schema Decoder class."""

    def __init__(self, schema: dict):
        self._schema = schema

    def build(self) -> type:
        """Build type from given avro schema item.

        :return: A dictionary object.
        """
        return self._build_type(self._schema)

    def _build_type(self, schema: Union[dict, list, str]) -> type:
        if isinstance(schema, dict):
            return self._build_type_from_dict(schema)
        elif isinstance(schema, list):
            return self._build_type_from_list(schema)
        else:
            return self._build_simple_type(schema)

    def _build_type_from_list(self, schema: list[Any]) -> type:
        options = tuple(self._build_type(entry) for entry in schema)
        return build_union(options)

    def _build_type_from_dict(self, schema: dict) -> type:
        if "logicalType" in schema:
            return self._build_logical_type(schema["logicalType"])
        elif schema["type"] == AVRO_ARRAY:
            return self._build_list_type(schema["items"])
        elif schema["type"] == AVRO_MAP:
            return self._build_dict_type(schema["values"])
        elif schema["type"] == AVRO_RECORD:
            return self._build_record_type(schema["name"], schema.get("namespace", None), schema["fields"])
        else:
            return self._build_type(schema["type"])

    @staticmethod
    def _build_logical_type(type_: str) -> type:
        if type_ == AVRO_DATE["logicalType"]:
            return date
        if type_ == AVRO_TIME["logicalType"]:
            return time
        if type_ == AVRO_TIMESTAMP["logicalType"]:
            return datetime
        if type_ == AVRO_UUID["logicalType"]:
            return UUID
        raise MinosMalformedAttributeException(f"Given logical field type is not supported: {type_!r}")

    def _build_list_type(self, items: Union[dict, str, Any] = None) -> type:
        return list[self._build_type(items)]

    def _build_dict_type(self, values: Union[dict, str, Any] = None) -> type:
        return dict[str, self._build_type(values)]

    def _build_record_type(self, name: str, namespace: Optional[str], fields: list[dict[str, Any]]) -> type:
        def _unpatch_namespace(mt: type) -> type:
            try:
                mt.namespace, _ = mt.namespace.rsplit(".", 1)
            except ValueError:
                pass
            return mt

        model_type = ModelType.build(
            name, {field["name"]: self._build_type(field["type"]) for field in fields}, namespace_=namespace
        )

        model_type = _unpatch_namespace(model_type)

        return model_type

    @staticmethod
    def _build_simple_type(type_: str) -> type:
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
