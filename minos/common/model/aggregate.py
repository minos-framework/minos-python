import dataclasses
import datetime
import typing
import uuid
import typing as t

from minos.common.model import types

BOOLEAN = "boolean"
NULL = "null"
INT = "int"
FLOAT = "float"
LONG = "long"
DOUBLE = "double"
BYTES = "bytes"
STRING = "string"
ARRAY = "array"
ENUM = "enum"
MAP = "map"
FIXED = "fixed"
DATE = "date"
TIME_MILLIS = "time-millis"
TIMESTAMP_MILLIS = "timestamp-millis"
UUID = "uuid"
DECIMAL = "decimal"

PYTHON_TYPE_TO_AVRO = {
    bool: BOOLEAN,
    type(None): NULL,
    int: LONG,
    float: DOUBLE,
    bytes: BYTES,
    str: STRING,
    list: ARRAY,
    tuple: ARRAY,
    dict: MAP,
    types.Fixed: {"type": FIXED},
    types.Enum: {"type": ENUM},
    datetime.date: {"type": INT, "logicalType": DATE},
    datetime.time: {"type": INT, "logicalType": TIME_MILLIS},
    datetime.datetime: {"type": LONG, "logicalType": TIMESTAMP_MILLIS},
    uuid.uuid4: {"type": STRING, "logicalType": UUID},
}

PYTHON_INMUTABLE_TYPES = (str, int, bool, float, bytes, type(None))
PYTHON_LIST_TYPES = (list, tuple)
PYTHON_ARRAY_TYPES = (dict, )

class AggregateField:
    __slots__ = "name", "type", "value"

    def __init__(self, name: str, type: t.Any, value: t.Optional[t.Any]):
        self.name = name
        self.type = type
        self.value = value

    def get_avro_type(self):
        """
        return the avro format of the field
        """
        origin = t.get_origin(self.type)
        if self.type in PYTHON_INMUTABLE_TYPES:
            return {"name": self.name, "type": PYTHON_TYPE_TO_AVRO[self.type]}

        # check with origin
        if origin in PYTHON_ARRAY_TYPES:
            args = t.get_args(self.type)
            type_dict = args[1]
            default_val = {}
            if self.value:
                default_val = self.value
            return {"name": self.name, "type": PYTHON_TYPE_TO_AVRO[origin],
                    "values": PYTHON_TYPE_TO_AVRO[type_dict], "default": default_val
            }

        if origin in PYTHON_LIST_TYPES:
            args = t.get_args(self.type)
            type_list = args[0]
            default_val = []
            if self.value:
                default_val = self.value
            return {"name": self.name, "type": PYTHON_TYPE_TO_AVRO[origin],
                    "items": PYTHON_TYPE_TO_AVRO[type_list], "default": default_val
                    }

        # case of Optional
        if isinstance(self.type, typing._UnionGenericAlias):
            # this is an optional value
            origin = t.get_origin(self.type)
            if origin is typing.Union:
                # this is an Optional value
                args = t.get_args(self.type)
                type_union = args[0]
                return {"name": self.name, "type": ["null", PYTHON_TYPE_TO_AVRO[type_union]]}

    def __repr__(self):
        return f"AggregateField(name={self.name}, type={self.type}, value={self.value})"


def _process_aggregate(cls):
    """
    Get the list of the class arguments and define it as an AggregateField class
    """
    cls_annotations = cls.__dict__.get('__annotations__', {})
    aggregate_fields = []
    for name, type in cls_annotations.items():
        attribute = getattr(cls, name, None)
        aggregate_fields.append(
            AggregateField(name=name, type=type, value=attribute)
        )
    setattr(cls, "_FIELDS", aggregate_fields)

    # g get metaclass
    meta_class = getattr(cls, "Meta", None)
    if meta_class:
        # meta class exist so get the informations related
        ...
    return cls


def aggregate(cls=None):
    def wrap(cls):
        return _process_aggregate(cls)

    if cls is None:
        return wrap

    return wrap(cls)


class MinosModel(object):
    __slots__ = "_fields"

    def __init__(self):
        self._fields = []

    def _get_fields(self):
        cls_annotations = self.__dict__.get('__annotations__', {})
