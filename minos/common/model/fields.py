"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import typing as t

from ..exceptions import MinosReqAttributeException, MinosTypeAttributeException, MinosMalformedAttributeException
from ..logs import log
from .types import ModelRef, MissingSentinel

PYTHON_INMUTABLE_TYPES = (str, int, bool, float, bytes)
PYTHON_LIST_TYPES = (list, tuple)
PYTHON_ARRAY_TYPES = (dict,)
PYTHON_NULL_TYPE = (type(None))

T = t.TypeVar("T")


class ModelField:
    """Represents a model field."""

    __slots__ = "_name", "_type", "_value", "_validator"

    def __init__(
        self,
        name: str, type_val: t.Type[T],
        value: T = MissingSentinel,
        validator: t.Optional[t.Callable[[t.Any], bool]] = None
    ):
        self._name = name
        self._type = type_val
        self._validator = validator
        self.value = value

    @property
    def name(self) -> str:
        """Name getter."""
        return self._name

    @property
    def type(self) -> t.Type:
        """Type getter."""
        return self._type

    @property
    def validator(self) -> t.Optional[t.Callable[[t.Any], T]]:
        """Parser getter."""
        return self._validator

    @property
    def _validator_name(self) -> t.Optional[str]:
        if self.validator is None:
            return None
        return self.validator.__name__

    @property
    def value(self) -> t.Any:
        """Value getter."""
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
            except (MinosTypeAttributeException, MinosReqAttributeException):
                pass

        if (data is None or data == MissingSentinel) and self._type != type(None):
            raise MinosReqAttributeException("")

        raise MinosTypeAttributeException(
            f"None of the '{alternatives}' alternatives matches the given data type: {type(data)}"
        )

    def _set_value(self, type_field: t.Type, data: t.Any) -> t.NoReturn:
        if type_field is type(None):
            if data is None or data == MissingSentinel:
                log.debug("the Value passed is None")
                self._value = None
                return
        else:
            if type_field in PYTHON_INMUTABLE_TYPES:
                if data is None:
                    raise MinosReqAttributeException(f"'{self.name}' field is 'None'.")
                elif data == MissingSentinel:
                    raise MinosReqAttributeException(f"'{self.name}' field is missing.")
                elif type_field is int and self._is_int(data):
                    log.debug("the Value passed is an integer")
                    self._value = int(data)
                    return
                elif type_field is bool and self._is_bool(data):
                    log.debug("the Value passed is an integer")
                    self._value = data
                    return
                elif type_field is str and self._is_string(data):
                    log.debug("the Value passed is a string")
                    self._value = data
                    return
            else:
                origin_type = t.get_origin(type_field)
                if origin_type is None:
                    raise MinosMalformedAttributeException(f"'{self.name}' field is malformed. Type: '{type_field}'.")
                if data is None:
                    raise MinosReqAttributeException(f"'{self.name}' field is 'None'.")
                elif data == MissingSentinel:
                    raise MinosReqAttributeException(f"'{self.name}' field is missing.")
                elif origin_type is list:
                    converted_data = self._is_list(data, t.get_args(type_field)[0], convert=True)
                    if isinstance(converted_data, bool) and not converted_data:
                        raise MinosTypeAttributeException(
                            f"{type(data)} could not be converted into {t.get_args(type_field)[0]} type"
                        )
                    self._value = converted_data
                    return

                elif origin_type is dict:
                    converted_data = self._is_dict(
                        data, t.get_args(type_field)[0], t.get_args(type_field)[1], convert=True
                    )
                    if isinstance(converted_data, bool) and not converted_data:
                        raise MinosTypeAttributeException(
                            f"{type(data)} could not be converted into {type_field} key, value types"
                        )
                    self._value = converted_data
                    return
                elif origin_type is ModelRef:
                    converted_data = self._is_model_ref(data, t.get_args(type_field)[0], convert=True)
                    if isinstance(converted_data, bool) and not converted_data:
                        raise MinosTypeAttributeException(
                            f"{type(data)} could not be converted into {t.get_args(type_field)[0]} type"
                        )
                    self._value = converted_data
                    return

        raise MinosTypeAttributeException(
            f"The '{type_field}' type does not match with the given data type: {type(data)}"
        )

    @staticmethod
    def _is_int(data: t.Union[int, str]) -> bool:
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

    @staticmethod
    def _is_string(data: str) -> bool:
        if isinstance(data, str):
            return True
        return False

    @staticmethod
    def _is_bool(data: bool) -> bool:
        if type(data) == bool:
            return True
        return False

    def _is_list(self, data: list, type_values: t.Any, convert: bool = False) -> t.Union[bool, list[t.Any]]:

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
    ) -> t.Union[bool, dict[t.Any, t.Any]]:

        if not isinstance(data, dict):
            return False
        # check if the values are instances of type_values
        data_converted = self._convert_dict_params(data, type_keys, type_values)
        if isinstance(data_converted, bool) and not data_converted:
            return False
        if convert:
            return data_converted
        return True

    def _convert_dict_params(
        self, data: t.Mapping, type_keys: t.Type, type_values: t.Type
    ) -> t.Union[bool, dict[t.Any, t.Any]]:
        keys = self._convert_list_params(data.keys(), type_keys)
        if isinstance(keys, bool) and not keys:
            return False

        values = self._convert_list_params(data.values(), type_values)
        if isinstance(values, bool) and not values:
            return False

        return dict(zip(keys, values))

    @staticmethod
    def _is_model_ref(data: t.Any, model_type: t.Type, convert: bool = False) -> t.Union[bool, t.Any]:
        if isinstance(data, model_type):
            if convert:
                return data
            return True
        return False

    def _convert_list_params(self, data: t.Iterable, type_params: t.Type) -> t.Union[bool, t.Any]:
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

    def __eq__(self, other: "ModelField") -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> t.Iterable:
        # noinspection PyRedundantParentheses
        yield from (
            self.name, self.type, self.value, self.validator
        )

    def __repr__(self):
        return f"ModelField(name={repr(self.name)}, type={repr(self.type)}, " \
               f"value={repr(self.value)}, validator={self._validator_name})"
