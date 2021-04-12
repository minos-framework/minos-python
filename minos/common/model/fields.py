import typing as t

from minos.common.exceptions import MinosModelAttributeException
from minos.common.logs import log

PYTHON_INMUTABLE_TYPES = (str, int, bool, float, bytes)
PYTHON_LIST_TYPES = (list, tuple)
PYTHON_ARRAY_TYPES = (dict,)
PYTHON_NULL_TYPE = (type(None))


class ModelField:
    __slots__ = "_name", "_type", "_value"

    def __init__(self, name: str, type_val: t.Any, value: t.Optional[t.Any] = None):
        self._name = name
        self._type = None
        origin = t.get_origin(type_val)

        log.debug(f"Name val {name}")
        log.debug(f"Origin Type {origin}")
        log.debug(f"Type val {type_val}")

        # get the type
        if type_val in PYTHON_INMUTABLE_TYPES:
            log.debug(f"Inmutable type for the Model Field")
            self._type = {"origin": type_val}
        if origin in PYTHON_LIST_TYPES or type_val in PYTHON_LIST_TYPES:
            log.debug(f"List or Tuple type for the Model Field")
            args = t.get_args(type_val)
            if len(args) == 0:
                log.debug(f"List type invalid")
                raise MinosModelAttributeException("List or Tuple need the argument type definition")
            self._type = {"origin": t.get_origin(type_val), "values": args[0]}
        if origin in PYTHON_ARRAY_TYPES or type_val in PYTHON_ARRAY_TYPES:
            log.debug(f"Dictionary type for the Model Field")
            args = t.get_args(type_val)
            if len(args) == 0 or len(args) == 1:
                raise MinosModelAttributeException("Dictionaries types need the key and value type definition")
            self._type = {"origin": t.get_origin(type_val), "keys": args[0], "values": args[1]}

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
        check if the value is correct
        """
        if "origin" in self._type:
            type_field = self._type['origin']
            if type_field is int and self._is_int(data):
                log.debug("the Value passed is an integer")
                self._value = int(data)
                return
            if type_field is bool and self._is_bool(data):
                log.debug("the Value passed is an integer")
                self._value = data
                return
            if type_field is str and self._is_string(data):
                log.debug("the Value passed is a string")
                self._value = data
                return
            if type_field is list:
                converted_list_data = self._is_list(data, self.type['values'], convert=True)
                if converted_list_data:
                    log.debug("the Value passed is a string")
                    self._value = converted_list_data
                    return
        raise MinosModelAttributeException(f"Something goes wrong check if the type of the passed value "
                                           f"is fine: data:{type(data)} vs type requested: {self._type['origin']}")

    def _is_int(self, data: t.Union[int, str]):
        if isinstance(data, str):
            # sometime the data is an integer but is passed as string, on that case would be better
            # to check if is a decimal
            try:
                int(data)
                return True
            except ValueError:
                return False
        if isinstance(data, int):
            return True
        return False

    def _is_string(self, data: str):
        if isinstance(data, str):
            return True
        return False

    def _is_bool(self, data: bool):
        if type(data) == bool:
            return True
        return False

    def _is_list(self, data: list, type_values: t.Any, convert: bool = False) -> t.Union[bool, t.List]:

        if isinstance(data, list):
            # check if the values are instances of type_values
            data_converted = self._convert_list_params(data, type_values)
            if data_converted:
                if convert:
                    return data_converted
                return True
        return False

    def _convert_list_params(self, data: list, type_params: t.Any):
        """
        check if the parameters list are equal to @type_params type
        """
        if type_params is int:
            # loop the list and check if all are integers
            log.debug("list parameters must be integer")
            converted = []
            for item in data:
                if self._is_int(item):
                    converted.append(int(item))
                else:
                    return False
            return converted

        if type_params is str:
            # loop the list and check if all are integers
            log.debug("list parameters must be string")
            for item in data:
                if self._is_string(item):
                    continue
                else:
                    return False
            return data

        if type_params is bool:
            # loop and chek is a boolean
            log.debug("list parameters must be boolean")
            for item in data:
                # loop the list and check if all are booleans
                if self._is_bool(item):
                    continue
                else:
                    return False
            return data

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
