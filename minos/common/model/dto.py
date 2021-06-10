"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import typing as t

from minos.common.model.abc.fields import (
    ModelField,
)
from minos.common.model.abc.model import (
    MinosModel,
)
from minos.common.protocol import (
    MinosAvroProtocol,
)


class DataTransferObject(MinosModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def from_avro_bytes(cls, raw: bytes, **kwargs) -> t.Union[DataTransferObject, list[DataTransferObject]]:
        """Build a single instance or a sequence of instances from bytes

        :param raw: A bytes data.
        :return: A single instance or a sequence of instances.
        """

        c = cls(**kwargs)
        m = MinosAvroProtocol
        schema = m.decode_schema(raw)
        decoded = m.decode(raw)

        for item in schema["fields"]:
            c._iterate_schema(item, decoded[item["name"]])

        return c

    def _iterate_schema(self, schema: tuple[list, t.Any], decoded: t.Union[dict[str, t.Any], list[dict[str, t.Any]]]):
        self.build_field(schema, decoded)

    def build_field(self, schema: dict, value: t.Any) -> t.NoReturn:
        """Build Model Fields from avro schema and value

        :param schema: Part of avro schema
        :param value: Decoded avro values
        """
        field_name = schema["name"]
        self._fields[field_name] = ModelField.from_avro(schema, value)

    @classmethod
    def from_typed_dict(cls, typed_dict: t.TypedDict, data: dict[str, t.Any]) -> DataTransferObject:
        """Build Model Fields form TypeDict object

        :param typed_dict: TypeDict object
        :param data: Data
        :return: DataTransferObject
        """
        fields = dict()
        for name, type_val in typed_dict.__annotations__.items():
            fields[name] = ModelField(name, type_val, data[name])
        c = cls()
        c._fields = fields
        return c
