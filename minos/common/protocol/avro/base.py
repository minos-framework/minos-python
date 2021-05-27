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
    """TODO"""

    @classmethod
    def encode(cls, value: Union[dict[str, Any], list[dict[str, Any]]], schema: dict[str, Any]) -> bytes:
        """Encoder in avro for database Values
        all the headers are converted in fields with double underscore name
        the body is a set fields coming from the data type.

        :param value: The data to be stored.
        :param schema: The schema relative to the data.
        :return: A bytes object.
        """
        if not isinstance(value, list):
            value = [value]
        try:
            schema_bytes = parse_schema(schema)
            with io.BytesIO() as file:
                writer(file, schema_bytes, value)
                file.seek(0)
                content_bytes = file.getvalue()
                return content_bytes
        except Exception:
            raise MinosProtocolException(
                "Error encoding data, check if is a correct Avro Schema and Datum is provided."
            )

    @classmethod
    def decode(cls, data: bytes, flatten: bool = True) -> Union[dict[str, Any], list[dict[str, Any]]]:
        """TODO

        :param data: TODO
        :param flatten: If ``True`` tries to return the values as flat as possible.
        :return: TODO
        """

        try:
            with io.BytesIO(data) as file:
                ans = list(reader(file))
        except Exception:
            raise MinosProtocolException("Error decoding string, check if is a correct Avro Binary data")

        if flatten and len(ans) == 1:
            return ans[0]

        return ans
