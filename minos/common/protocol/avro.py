"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import io
import typing as t
import avro
import orjson
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from ..exceptions import MinosProtocolException
from ..logs import log
from .abstract import MinosBinaryProtocol


class MinosAvroProtocol(MinosBinaryProtocol):


    @classmethod
    def encode(cls, headers: t.Dict, body: t.Any = None) -> bytes:
        """
        encoder in avro
        all the headers are converted in fields with doble underscore name
        the body is a set fields coming from the data type.
        """

        #prepare the headers
        schema = {
            "type": "record",
            "namespace": 'org.minos.protocol',
            "name": "message",
            "fields": [
                {
                    "name": "headers",
                    "type": {
                        "type": "map",
                        "values": ["string", "int", "bytes", "long", "float", "boolean"]
                    }
                },
                {
                    "name": "body",
                    "type": [
                        "null", "string", "int", "bytes", "boolean", "float", "long",
                        {
                            "type": "array",
                            "items": ["int", "string", "bytes", "long", "boolean", "float"]
                        },
                         {
                             "type": "map",
                             "values": [
                                 "null", "int", "string", "bytes", "boolean", "float", "long",
                                 {"type": "array", "items": [
                                     "string", "int", "bytes", "long", "boolean", "float",
                                     {"type": "map", "values": ["string", "int", "bytes", "long", "boolean", "float"]}
                                 ]},
                                 {"type": "map", "values": ["string", "int", "bytes", "long", "boolean", "float",
                                                            {"type": "map", "values": ["string", "int", "bytes", "long",
                                                                                       "boolean", "float"]}]}
                             ]
                         }
                    ]
                }
            ]
        }
        schema_json = orjson.dumps(schema)
        schema_bytes = avro.schema.parse(schema_json)
        final_data = {}
        final_data['headers'] = headers
        if body:
            final_data['body'] = body
        with io.BytesIO() as f:
            writer = DataFileWriter(f, DatumWriter(), schema_bytes)
            writer.append(final_data)
            writer.flush()
            f.seek(0)
            content_bytes = f.getvalue()
            writer.close()
            return content_bytes


    @classmethod
    def decode(cls, data: bytes) -> t.Dict:
        data_return: t.Dict = {}
        try:
            message_avro = DataFileReader(io.BytesIO(data), DatumReader())
            data_return['headers'] = {}
            for schema_dict in message_avro:
                log.debug(f"Avro: get the request/response in dict format")
                data_return['headers'] = schema_dict['headers']
                # check wich type is body
                if "body" in schema_dict:
                    if isinstance(schema_dict['body'], dict):
                        data_return['body'] = {}
                    elif isinstance(schema_dict['body'], list):
                        data_return['body'] = []
                    data_return['body'] = schema_dict['body']
            return data_return
        except Exception:
            raise MinosProtocolException("Error decoding string, check if is a correct Avro Binary data")


class MinosAvroValuesDatabase(MinosBinaryProtocol):

    @classmethod
    def encode(cls, value: t.Any, schema: t.Dict = None) -> bytes:
        """
        encoder in avro for database Values
        all the headers are converted in fields with doble underscore name
        the body is a set fields coming from the data type.
        """

        # prepare the headers
        if not schema:
            schema_val = {
                "type": "record",
                "namespace": 'org.minos.protocol.database',
                "name": "value",
                "fields": [
                    {
                        "name": "content",
                        "type": [
                            "null", "string", "int", "bytes", "boolean", "float", "long",
                            {
                                "type": "array",
                                "items": ["int", "string", "bytes", "long", "boolean", "float"]
                            },
                            {
                                "type": "map",
                                "values": [
                                    "null", "int", "string", "bytes", "boolean", "float", "long",
                                    {"type": "array", "items": [
                                        "string", "int", "bytes", "long", "boolean", "float",
                                        {"type": "map", "values": ["string", "int", "bytes", "long", "boolean", "float"]}
                                    ]},
                                    {"type": "map", "values": ["string", "int", "bytes", "long", "boolean", "float",
                                                               {"type": "map",
                                                                "values": ["string", "int", "bytes", "long",
                                                                           "boolean", "float"]}]}
                                ]
                            }
                        ]
                    }
                ]
            }
            final_data = {}
            final_data['content'] = value
        else:
            schema_val = schema
            final_data = value

        if not isinstance(final_data, list):
            final_data = [final_data]

        schema_json = orjson.dumps(schema_val)
        schema_bytes = avro.schema.parse(schema_json)

        with io.BytesIO() as f:
            writer = DataFileWriter(f, DatumWriter(), schema_bytes)
            for d in final_data:
                writer.append(d)
            writer.flush()
            f.seek(0)
            content_bytes = f.getvalue()
            writer.close()
            return content_bytes

    @classmethod
    def decode(cls, data: bytes, content_root: bool = True) -> t.Union[dict[str, t.Any], list[dict[str, t.Any]]]:
        try:
            ans = list()
            message_avro = DataFileReader(io.BytesIO(data), DatumReader())
            for schema_dict in message_avro:
                log.debug(f"Avro Database: get the values data")
                if content_root:
                    schema_dict = schema_dict['content']
                ans.append(schema_dict)
            if len(ans) == 1:
                return ans[0]
            return ans
        except Exception:
            raise MinosProtocolException("Error decoding string, check if is a correct Avro Binary data")
