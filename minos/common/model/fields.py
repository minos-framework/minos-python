"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import typing as t

from ..exceptions import MinosModelAttributeException
from ..logs import log
from .types import ModelRef

PYTHON_INMUTABLE_TYPES = (str, int, bool, float, bytes)
PYTHON_LIST_TYPES = (list, tuple)
PYTHON_ARRAY_TYPES = (dict,)
PYTHON_NULL_TYPE = (type(None))

T = t.TypeVar("T")


class ModelField:
    __slots__ = "_name", "_type", "_value"

    def __init__(self, name: str, type_val: t.Type[T], value: T):
        self._name = name
        self._type = None
        self._type = type_val
        self.value = value

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
    def value(self, data: t.Any) -> t.NoReturn:
        """
        check if the value is correct
        """
        log.debug(f"Name val {self._name}")
        log.debug(f"Type val {self._type}")

        origin = t.get_origin(self._type)
        log.debug(f"Origin Type {origin}")

        if origin is not t.Union:
            self._set_value(self._type, data)
            return

        alternatives = t.get_args(self._type)
        for alternative_type in alternatives:
            try:
                self._set_value(alternative_type, data)
                return
            except MinosModelAttributeException:
                pass

        raise MinosModelAttributeException(
            f"None of the '{alternatives}' alternatives matches the given data type: {type(data)}"
        )

    def _set_value(self, type_field: t.Type, data: t.Any) -> t.NoReturn:
        if type_field is type(None) and data is None:
            log.debug("the Value passed is None")
            self._value = None
        elif type_field is int and self._is_int(data):
            log.debug("the Value passed is an integer")
            self._value = int(data)
        elif type_field is bool and self._is_bool(data):
            log.debug("the Value passed is an integer")
            self._value = data
            return
        elif type_field is str and self._is_string(data):
            log.debug("the Value passed is a string")
            self._value = data
        elif t.get_origin(type_field) is list:
            converted_data = self._is_list(data, t.get_args(type_field)[0], convert=True)
            if isinstance(converted_data, bool) and not converted_data:
                raise MinosModelAttributeException(
                    f"{type(data)} could not be converted into {t.get_args(type_field)[0]} type"
                )
            self._value = converted_data
        elif type_field is dict or t.get_origin(type_field) is dict:
            converted_data = self._is_dict(data, t.get_args(type_field)[0], t.get_args(type_field)[1], convert=True)
            if isinstance(converted_data, bool) and not converted_data:
                raise MinosModelAttributeException(
                    f"{type(data)} could not be converted into {type_field} key, value types"
                )
            self._value = converted_data
        elif type_field is ModelRef or t.get_origin(type_field) is ModelRef:
            converted_data = self._is_model_ref(data, t.get_args(type_field)[0], convert=True)

            if isinstance(converted_data, bool) and not converted_data:
                raise MinosModelAttributeException(
                    f"{type(data)} could not be converted into {t.get_args(type_field)[0]} type"
                )
            self._value = converted_data
        else:
            raise MinosModelAttributeException(
                f"Something goes wrong check if the type of the passed value "
                f"is fine: data:{type(data)} vs type requested: {type_field}"
            )

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

    def _is_dict(
        self, data: list, type_keys: t.Type, type_values: t.Type, convert: bool = False
    ) -> t.Union[bool, t.Dict]:

        if not isinstance(data, dict):
            return False
        # check if the values are instances of type_values
        data_converted = self._convert_dict_params(data, type_keys, type_values)
        if isinstance(data_converted, bool) and not data_converted:
            return False
        if convert:
            return data_converted
        return True

    def _convert_dict_params(self, data: t.Mapping, type_keys: t.Type, type_values: t.Type) -> t.Union[bool, t.Dict]:
        keys = self._convert_list_params(data.keys(), type_keys)
        if isinstance(keys, bool) and not keys:
            return False

        values = self._convert_list_params(data.values(), type_values)
        if isinstance(values, bool) and not values:
            return False

        return dict(zip(keys, values))

    @staticmethod
    def _is_model_ref(data: t.Any, model_type: t.Any, convert: bool = False) -> t.Union[bool, t.Any]:
        if isinstance(data, model_type):
            if convert:
                return data
            return True
        return False

    def _convert_list_params(self, data: t.Iterable, type_params: t.Any) -> t.Union[bool, t.List]:
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
