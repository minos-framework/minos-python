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
    """TODO"""

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
    def from_avro(cls, schema: dict[str, Any], data: dict[str, Any]) -> T:
        """TODO

        :param schema: TODO
        :param data: TODO
        :return: TODO
        """
        fields = dict()
        for raw in schema["fields"]:
            fields[raw["name"]] = ModelField.from_avro(raw, data[raw["name"]])
        return cls(fields)

    def _type_hints(self) -> dict[str, Any]:
        yield from ((field.name, field.type) for field in self.fields.values())
