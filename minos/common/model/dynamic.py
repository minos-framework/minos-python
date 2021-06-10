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

from ..meta import (
    self_or_classmethod,
)
from ..protocol import (
    MinosAvroProtocol,
)
from .abc import (
    MinosModel,
)
from .fields import (
    ModelField,
)

logger = logging.getLogger(__name__)


def _diff(a: dict, b: dict) -> dict:
    d = set(a.items()) - set(b.items())
    return dict(d)


T = TypeVar("T")


class DynamicMinosModel(MinosModel):
    """TODO"""

    def __init__(self, fields: dict[str, ModelField]):
        self._fields = fields

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

    # noinspection PyMethodParameters
    @self_or_classmethod
    def _type_hints(self_or_cls) -> dict[str, Any]:
        if isinstance(self_or_cls, type):
            return dict()
        yield from ((field.name, field.type) for field in self_or_cls.fields.values())
