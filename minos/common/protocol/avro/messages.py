"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import logging
from typing import (
    Any,
)

from .base import (
    MinosAvroProtocol,
)

logger = logging.getLogger(__name__)


class MinosAvroMessageProtocol(MinosAvroProtocol):
    """Minos Avro Messages Protocol class."""

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

        return super().encode(final_data, _AVRO_SCHEMA)

    @classmethod
    def decode(cls, data: bytes, *args, **kwargs) -> dict:
        """Decode the given bytes of data into a single dictionary or a sequence of dictionaries.

        :param data: A bytes object.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A dictionary or a list of dictionaries.
        """
        data_return = {"headers": dict()}
        for schema_dict in super().decode(data, flatten=False):
            logger.debug("Avro: get the request/response in dict format")
            data_return["headers"] = schema_dict["headers"]
            # check wich type is body
            if "body" in schema_dict:
                if isinstance(schema_dict["body"], dict):
                    data_return["body"] = {}
                elif isinstance(schema_dict["body"], list):
                    data_return["body"] = []
                data_return["body"] = schema_dict["body"]
            return data_return


_AVRO_SCHEMA = {
    "type": "record",
    "namespace": "org.minos.protocol",
    "name": "message",
    "fields": [
        {"name": "headers", "type": {"type": "map", "values": ["string", "int", "bytes", "long", "float", "boolean"]}},
        {
            "name": "body",
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
        },
    ],
}
