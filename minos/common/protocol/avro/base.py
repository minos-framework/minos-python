"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import io
from typing import (
    Any,
    Union,
)

from fastavro import (
    parse_schema,
    reader,
    writer,
)

from ...exceptions import (
    MinosProtocolException,
)
from ..abc import (
    MinosBinaryProtocol,
)


class MinosAvroProtocol(MinosBinaryProtocol):
    """Minos Avro Protocol class."""

    @classmethod
    def encode(
        cls,
        value: Union[dict[str, Any], list[dict[str, Any]]],
        schema: Union[dict[str, Any], list[dict[str, Any]]],
        *args,
        **kwargs,
    ) -> bytes:
        """Encoder in avro for database Values
        all the headers are converted in fields with double underscore name
        the body is a set fields coming from the data type.

        :param value: The data to be stored.
        :param schema: The schema relative to the data.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A bytes object.
        """
        if not isinstance(value, list):
            value = [value]
        if not isinstance(schema, list):
            schema = [schema]

        try:
            raw_schema = cls._parse_schema(schema)
            return cls._write_data(value, raw_schema)
        except Exception as exc:
            raise MinosProtocolException(f"Error encoding data: {exc!r}")

    @staticmethod
    def _parse_schema(schema: list[dict[str, Any]]) -> dict[str, Any]:
        named_schemas = {}
        for item in schema[:-1]:
            parse_schema(item, named_schemas)
        return parse_schema(schema[-1], named_schemas, expand=True)

    @staticmethod
    def _write_data(value: list[dict[str, Any]], schema: dict[str, Any]):
        with io.BytesIO() as file:
            writer(file, schema, value)
            file.seek(0)
            content = file.getvalue()
        return content

    @classmethod
    def decode(cls, data: bytes, flatten: bool = True, *args, **kwargs) -> Union[dict[str, Any], list[dict[str, Any]]]:
        """Decode the given bytes of data into a single dictionary or a sequence of dictionaries.

        :param data: A bytes object.
        :param flatten: If ``True`` tries to return the values as flat as possible.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A dictionary or a list of dictionaries.
        """

        try:
            with io.BytesIO(data) as file:
                ans = list(reader(file))
        except Exception as exc:
            raise MinosProtocolException(f"Error decoding the avro bytes: {exc}")

        if flatten and len(ans) == 1:
            return ans[0]

        return ans

    @classmethod
    def decode_schema(cls, data: bytes, *args, **kwargs) -> Union[dict[str, Any], list[dict[str, Any]]]:
        """Decode the given bytes of data into a single dictionary or a sequence of dictionaries.

        :param data: A bytes object.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A tuple or a list of tuples.
        """

        try:
            with io.BytesIO(data) as file:
                r = reader(file)
                schema = r.writer_schema

        except Exception as exc:
            raise MinosProtocolException(f"Error getting avro schema: {exc}")

        return schema
