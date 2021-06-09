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
    def from_avro_bytes(self, raw: bytes, **kwargs) -> DataTransferObject:
        """Build a single instance or a sequence of instances from bytes

        :param raw: A bytes data.
        :return: A single instance or a sequence of instances.
        """

        schema = MinosAvroProtocol.decode_schema(raw)
        decoded = MinosAvroProtocol.decode(raw)
        for item in schema["fields"]:
            self.build_field(item, decoded[item["name"]])

        return self

    def build_field(self, schema: dict, value: t.Any) -> t.NoReturn:
        field_name = schema["name"]
        self._fields[field_name] = ModelField.from_avro(schema, value)
