"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from typing import (
    Any,
    Union,
)

from .base import (
    MinosAvroProtocol,
)


class MinosAvroDatabaseProtocol(MinosAvroProtocol):
    """Encoder/Decoder class for values to be stored on the database with avro format."""

    @classmethod
    def encode(cls, value: Any, *args, **kwargs) -> bytes:
        """Encoder in avro for database Values
        all the headers are converted in fields with double underscore name
        the body is a set fields coming from the data type.

        :param value: The data to be stored.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A bytes object.
        """

        # prepare the headers
        final_data = dict()
        final_data["content"] = value

        return super().encode(final_data, _AVRO_SCHEMA)

    @classmethod
    def decode(cls, data: bytes, flatten: bool = True, *args, **kwargs) -> Union[dict[str, Any], list[dict[str, Any]]]:
        """Decode the given bytes of data into a single dictionary or a sequence of dictionaries.

        :param data: A bytes object.
        :param flatten: If ``True`` tries to return the values as flat as possible.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A dictionary or a list of dictionaries.
        """
        schema_dict = super().decode(data)
        return schema_dict["content"]


_AVRO_SCHEMA = {
    "type": "record",
    "namespace": "org.minos.protocol.database",
    "name": "value",
    "fields": [
        {
            "name": "content",
            "type": [
                "null",
                "string",
                "int",
                "bytes",
                "boolean",
                "float",
                "long",
                {"type": "array", "items": ["int", "string", "bytes", "long", "boolean", "float"]},
                {
                    "type": "map",
                    "values": [
                        "null",
                        "int",
                        "string",
                        "bytes",
                        "boolean",
                        "float",
                        "long",
                        {
                            "type": "array",
                            "items": [
                                "string",
                                "int",
                                "bytes",
                                "long",
                                "boolean",
                                "float",
                                {"type": "map", "values": ["string", "int", "bytes", "long", "boolean", "float"]},
                            ],
                        },
                        {
                            "type": "map",
                            "values": [
                                "string",
                                "int",
                                "bytes",
                                "long",
                                "boolean",
                                "float",
                                {"type": "map", "values": ["string", "int", "bytes", "long", "boolean", "float"]},
                            ],
                        },
                    ],
                },
            ],
        }
    ],
}
