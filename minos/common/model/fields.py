import logging
import typing as t

from minos.common.exceptions import MinosModelAttributeException

PYTHON_INMUTABLE_TYPES = (str, int, bool, float, bytes)
PYTHON_LIST_TYPES = (list, tuple)
PYTHON_ARRAY_TYPES = (dict,)
PYTHON_NULL_TYPE = (type(None))


class ModelField:
    __slots__ = "_name", "_type", "_value"

    def __init__(self, name: str, type_val: t.Any, value: t.Optional[t.Any]):
        self._name = name
        self._type = None
        # get the type
        if type_val in PYTHON_INMUTABLE_TYPES:
            self._type = {"origin": type_val}
        if t.get_origin(type_val) in PYTHON_LIST_TYPES:
            args = t.get_args(type_val)
            logging.debug(f"Pass For a List {args}")
            if len(args) == 0:
                raise MinosModelAttributeException("List or Tuple need the argument type definition")
            self._type = {"origin": t.get_origin(type_val), "values": args[0]}
        if t.get_origin(type_val) in PYTHON_ARRAY_TYPES:
            logging.debug("Pass For a Dict")
            args = t.get_args(type_val)
            if len(args) == 0 or len(args) == 1:
                raise MinosModelAttributeException("Dictionaries types need the key and value type definition")
            self._type = {"origin": t.get_origin(type_val), "values": args[1]}

        self._value = value

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, data: t.Any):
        """
        The value setter check if
        """
        self._value = data

    # def get_avro(self):
    #     """
    #     return the avro format of the field
    #     """
    #     origin = t.get_origin(self.type)
    #     if self.type in PYTHON_INMUTABLE_TYPES:
    #         return {"name": self.name, "type": PYTHON_TYPE_TO_AVRO[self.type]}
    #
    #     # check with origin
    #     if origin in PYTHON_ARRAY_TYPES:
    #         args = t.get_args(self.type)
    #         type_dict = args[1]
    #         default_val = {}
    #         if self.value:
    #             default_val = self.value
    #         return {"name": self.name, "type": PYTHON_TYPE_TO_AVRO[origin],
    #                 "values": PYTHON_TYPE_TO_AVRO[type_dict], "default": default_val
    #         }
    #
    #     if origin in PYTHON_LIST_TYPES:
    #         args = t.get_args(self.type)
    #         type_list = args[0]
    #         default_val = []
    #         if self.value:
    #             default_val = self.value
    #         return {"name": self.name, "type": PYTHON_TYPE_TO_AVRO[origin],
    #                 "items": PYTHON_TYPE_TO_AVRO[type_list], "default": default_val
    #                 }
    #
    #     # case of Optional
    #     if isinstance(self.type, typing._UnionGenericAlias):
    #         # this is an optional value
    #         origin = t.get_origin(self.type)
    #         if origin is typing.Union:
    #             # this is an Optional value
    #             args = t.get_args(self.type)
    #             type_union = args[0]
    #             return {"name": self.name, "type": ["null", PYTHON_TYPE_TO_AVRO[type_union]]}

    def __repr__(self):
        return f"AggregateField(name={self.name}, type={self.type}, value={self.value})"
