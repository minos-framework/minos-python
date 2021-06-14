"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from typing import (
    Any,
    TypeVar,
    Union,
)

from ...meta import (
    self_or_classmethod,
)
from ...protocol import (
    MinosAvroProtocol,
)
from ..abc import (
    Model,
)
from ..fields import (
    ModelField,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class DynamicModel(Model):
    """Base class for ``minos`` dynamic model entities"""

    def __init__(self, fields, **kwargs):
        super().__init__(fields)

    @classmethod
    def from_avro_bytes(cls, raw: bytes, **kwargs) -> Union[T, list[T]]:
        """Build a single instance or a sequence of instances from bytes

        :param raw: A bytes data.
        :return: A single instance or a sequence of instances.
        """

        schema = MinosAvroProtocol.decode_schema(raw)
        decoded = MinosAvroProtocol.decode(raw)
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
            schema = schema[0]
        schema = dict(schema)  # To avoid collateral effects related with the schema modification.
        fields = dict()
        for raw in schema.pop("fields"):
            fields[raw["name"]] = ModelField.from_avro(raw, data[raw["name"]])
        return cls(fields=fields, **schema)

    @self_or_classmethod
    def _type_hints(self) -> dict[str, Any]:
        yield from ((field.name, field.type) for field in self.fields.values())
