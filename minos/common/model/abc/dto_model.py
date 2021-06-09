"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import typing as t

from ...protocol import (
    MinosAvroProtocol,
)
from .fields import (
    ModelField,
)
from .model import (
    MinosModel,
)


class DtoModel(MinosModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def from_avro_bytes(self, raw: bytes, **kwargs) -> t.Type[DtoModel]:
        """Build a single instance or a sequence of instances from bytes

        :param raw: A bytes data.
        :return: A single instance or a sequence of instances.
        """

        schema = MinosAvroProtocol().decode_schema(raw)
        decoded = MinosAvroProtocol().decode(raw)
        for item in schema["fields"]:
            self.build_field(item, decoded[item["name"]])

    def build_field(self, schema: dict, value: t.Any) -> t.Type[DtoModel]:
        field_name = schema["name"]
        self._fields[field_name] = ModelField.from_avro(schema, value)
