"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import inspect
import typing as t

from ..exceptions import (
    MinosAttributeValidationException,
    MinosMalformedAttributeException,
    MinosParseAttributeException,
    MinosReqAttributeException,
    MinosTypeAttributeException,
)
from ..logs import log
from .types import (
    MissingSentinel,
    ModelRef,
)

PYTHON_IMMUTABLE_TYPES = (str, int, bool, float, bytes)
PYTHON_LIST_TYPES = (list, tuple)
PYTHON_ARRAY_TYPES = (dict,)
PYTHON_NULL_TYPE = type(None)

T = t.TypeVar("T")


class ModelField:
    """Represents a model field."""

    __slots__ = "_name", "_type", "_value", "_parser", "_validator"

    def __init__(
        self,
        name: str,
        type_val: t.Type[T],
        value: T = MissingSentinel,
        parser: t.Optional[t.Callable[[t.Any], T]] = None,
        validator: t.Optional[t.Callable[[t.Any], bool]] = None,
    ):
        self._name = name
        self._type = type_val
        self._parser = parser
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
    def parser(self) -> t.Optional[t.Callable[[t.Any], T]]:
        """Parser getter."""
        return self._parser

    @property
    def _parser_name(self) -> t.Optional[str]:
        if self.parser is None:
            return None
        return self.parser.__name__

    @property
    def _parser_function(self) -> t.Optional[t.Callable[[t.Any], T]]:
        if self.parser is None:
            return None
        if inspect.ismethod(self.parser):
            # noinspection PyUnresolvedReferences
            return self.parser.__func__
        return self.parser

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
    def _validator_function(self) -> t.Optional[t.Callable[[t.Any], T]]:
        if self.validator is None:
            return None
        if inspect.ismethod(self.validator):
            # noinspection PyUnresolvedReferences
            return self.validator.__func__

    @property
    def value(self) -> t.Any:
        """Value getter."""
        return self._value

    @value.setter
    def value(self, data: t.Any) -> t.NoReturn:
        """Check if the given value is correct and stores it if ``True``, otherwise raises an exception.

        :param data: new value.
        :return: This method does not return anything.
        """
        if self._parser is not None:
            try:
                data = self.parser(data)
            except Exception as exc:
                raise MinosParseAttributeException(self.name, data, exc)

        log.debug(f"Name val {self._name}")
        log.debug(f"Type val {self._type}")

        value = _ModelFieldCaster(self).cast(data)

        if self.validator is not None and value is not None and not self.validator(value):
            raise MinosAttributeValidationException(self.name, value)

        self._value = value

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
        yield from (self.name, self.type, self.value, self._parser_function, self._validator_function)

    def __repr__(self):
        return (
            f"ModelField(name={repr(self.name)}, type={repr(self.type)}, value={repr(self.value)}, "
            f"parser={self._parser_name}, validator={self._validator_name})"
        )


class _ModelFieldCaster(object):
    def __init__(self, field: ModelField):
        self._field = field

    @property
    def _name(self):
        return self._field.name

    @property
    def _type(self):
        return self._field.type

    def cast(self, data: t.Any) -> t.Any:
        """Cast data type according to the field definition..

        :param data: Data to be casted.
        :return: Casted object.
        """
        return self._cast_value(self._type, data)

    def _cast_value(self, type_field: t.Type, data: t.Any) -> t.Any:
        origin = t.get_origin(type_field)
        if origin is not t.Union:
            return self._cast_single_value(type_field, data)
        return self._cast_union_value(type_field, data)

    def _cast_union_value(self, type_field: t.Type, data: t.Any) -> t.Any:
        alternatives = t.get_args(type_field)
        for alternative_type in alternatives:
            try:
                return self._cast_single_value(alternative_type, data)
            except (MinosTypeAttributeException, MinosReqAttributeException):
                pass

        if type_field is not type(None):
            if data is None:
                raise MinosReqAttributeException(f"'{self._name}' field is 'None'.")

            if data is MissingSentinel:
                raise MinosReqAttributeException(f"'{self._name}' field is missing.")

        raise MinosTypeAttributeException(self._name, type_field, data)

    def _cast_single_value(self, type_field: t.Type, data: t.Any) -> t.Any:
        if type_field is type(None):
            return self._cast_none_value(type_field, data)

        if type_field in PYTHON_IMMUTABLE_TYPES:
            return self._cast_simple_value(type_field, data)

        return self._cast_composed_value(type_field, data)

    def _cast_none_value(self, type_field: t.Type, data: t.Any) -> t.Any:
        if data is None or data is MissingSentinel:
            log.debug("the Value passed is None")
            return None

        raise MinosTypeAttributeException(self._name, type_field, data)

    def _cast_simple_value(self, type_field: t.Type, data: t.Any) -> t.Any:
        if data is None:
            raise MinosReqAttributeException(f"'{self._name}' field is 'None'.")

        if data is MissingSentinel:
            raise MinosReqAttributeException(f"'{self._name}' field is missing.")

        if type_field is int:
            log.debug("the Value passed is an integer")
            return self._cast_int(data)

        if type_field is float:
            log.debug("the Value passed is an integer")
            return self._cast_float(data)

        if type_field is bool:
            log.debug("the Value passed is an integer")
            return self._cast_bool(data)

        if type_field is str:
            log.debug("the Value passed is a string")
            return self._cast_string(data)

        raise MinosTypeAttributeException(self._name, type_field, data)  # pragma: no cover

    def _cast_int(self, data: t.Any) -> int:
        try:
            return int(data)
        except (ValueError, TypeError):
            raise MinosTypeAttributeException(self._name, int, data)

    def _cast_float(self, data: t.Any) -> float:
        try:
            return float(data)
        except (ValueError, TypeError):
            raise MinosTypeAttributeException(self._name, float, data)

    def _cast_bool(self, data: t.Any) -> bool:
        if not isinstance(data, bool):
            raise MinosTypeAttributeException(self._name, bool, data)
        return data

    def _cast_string(self, data: t.Any) -> str:
        if not isinstance(data, str):
            raise MinosTypeAttributeException(self._name, str, data)
        return data

    def _cast_composed_value(self, type_field: t.Type, data: t.Any) -> t.Any:
        origin_type = t.get_origin(type_field)
        if origin_type is None:
            raise MinosMalformedAttributeException(f"'{self._name}' field is malformed. Type: '{type_field}'.")

        if data is None:
            raise MinosReqAttributeException(f"'{self._name}' field is 'None'.")

        if data is MissingSentinel:
            raise MinosReqAttributeException(f"'{self._name}' field is missing.")

        if origin_type is list:
            return self._convert_list(data, t.get_args(type_field)[0])

        if origin_type is dict:
            return self._convert_dict(data, t.get_args(type_field)[0], t.get_args(type_field)[1])

        if origin_type is ModelRef:
            return self._convert_model_ref(data, t.get_args(type_field)[0])

        raise MinosTypeAttributeException(self._name, type_field, data)

    def _convert_list(self, data: list, type_values: t.Any) -> list[t.Any]:
        if not isinstance(data, list):
            raise MinosTypeAttributeException(self._name, list, data)

        return self._convert_list_params(data, type_values)

    def _convert_dict(self, data: list, type_keys: t.Type, type_values: t.Type) -> dict[t.Any, t.Any]:
        if not isinstance(data, dict):
            raise MinosTypeAttributeException(self._name, dict, data)

        return self._convert_dict_params(data, type_keys, type_values)

    def _convert_dict_params(self, data: t.Mapping, type_keys: t.Type, type_values: t.Type) -> dict[t.Any, t.Any]:
        keys = self._convert_list_params(data.keys(), type_keys)
        values = self._convert_list_params(data.values(), type_values)
        return dict(zip(keys, values))

    def _convert_model_ref(self, data: t.Any, model_type: t.Type) -> t.Any:
        if not isinstance(data, model_type):
            raise MinosTypeAttributeException(self._name, model_type, data)
        return data

    def _convert_list_params(self, data: t.Iterable, type_params: t.Type) -> list[t.Any]:
        """
        check if the parameters list are equal to @type_params type
        """
        converted = list()
        for item in data:
            value = self._cast_value(type_params, item)
            converted.append(value)
        return converted
