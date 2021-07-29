"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
AVRO_BOOLEAN = "boolean"
AVRO_NULL = "null"
AVRO_INT = "int"
AVRO_FLOAT = "float"
AVRO_LONG = "long"
AVRO_DOUBLE = "double"
AVRO_BYTES = "bytes"
AVRO_STRING = "string"
AVRO_ARRAY = "array"
AVRO_RECORD = "record"
AVRO_ENUM = "enum"
AVRO_MAP = "map"
AVRO_FIXED = "fixed"
AVRO_DECIMAL = "decimal"

AVRO_DATE = {"type": AVRO_INT, "logicalType": "date"}
AVRO_TIME = {"type": AVRO_INT, "logicalType": "time-micros"}
AVRO_TIMESTAMP = {"type": AVRO_LONG, "logicalType": "timestamp-micros"}
AVRO_UUID = {"type": AVRO_STRING, "logicalType": "uuid"}
