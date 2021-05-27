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
from ...logs import (
    log,
)
from ..abc import (
    MinosBinaryProtocol,
)
from .schemas import (
    DATABASE_AVRO_SCHEMA,
    MESSAGE_AVRO_SCHEMA,
)


class MinosAvroProtocol(MinosBinaryProtocol):
    """TODO"""

    @classmethod
    def encode(cls, value: Union[dict[str, Any], list[dict[str, Any]]], schema: list[dict[str, Any]]) -> bytes:
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


class MinosAvroMessageProtocol(MinosAvroProtocol):
    """TODO"""

    @classmethod
    def encode(cls, headers: dict, body: Any = None, *args, **kwargs) -> bytes:
        """
        encoder in avro
        all the headers are converted in fields with double underscore name
        the body is a set fields coming from the data type.
        """

        # prepare the headers
        final_data = dict()
        final_data["headers"] = headers
        if body:
            final_data["body"] = body

        return super().encode(final_data, MESSAGE_AVRO_SCHEMA)

    @classmethod
    def decode(cls, data: bytes, *args, **kwargs) -> dict:
        """TODO

        :param data: TODO
        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        data_return = {"headers": dict()}
        for schema_dict in super().decode(data, flatten=False):
            log.debug("Avro: get the request/response in dict format")
            data_return["headers"] = schema_dict["headers"]
            # check wich type is body
            if "body" in schema_dict:
                if isinstance(schema_dict["body"], dict):
                    data_return["body"] = {}
                elif isinstance(schema_dict["body"], list):
                    data_return["body"] = []
                data_return["body"] = schema_dict["body"]
            return data_return


class MinosAvroDatabaseProtocol(MinosAvroProtocol):
    """Encoder/Decoder class for values to be stored on the database with avro format."""

    @classmethod
    def encode(cls, value: Any, *args, **kwargs) -> bytes:
        """Encoder in avro for database Values
        all the headers are converted in fields with double underscore name
        the body is a set fields coming from the data type.

        :param value: The data to be stored.
        :return: A bytes object.
        """

        # prepare the headers
        final_data = dict()
        final_data["content"] = value

        return super().encode(final_data, DATABASE_AVRO_SCHEMA)

    @classmethod
    def decode(cls, data: bytes, flatten: bool = True) -> Union[dict[str, Any], list[dict[str, Any]]]:
        """Decode the given bytes of data into a single dictionary or a sequence of dictionaries.

        :param data: A bytes object.
        :param flatten: If ``True`` tries to return the values as flat as possible.
        :return: A dictionary or a list of dictionaries.
        """
        ans = list()
        for schema_dict in super().decode(data, flatten=False):
            log.debug("Avro Database: get the values data")
            schema_dict = schema_dict["content"]
            ans.append(schema_dict)
        if flatten and len(ans) == 1:
            return ans[0]
        return ans
