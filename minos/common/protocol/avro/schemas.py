"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
DATABASE_AVRO_SCHEMA = {
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

MESSAGE_AVRO_SCHEMA = {
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
