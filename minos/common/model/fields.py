"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import inspect
import typing as t

from ..exceptions import MinosAttributeValidationException
from ..exceptions import MinosMalformedAttributeException
from ..exceptions import MinosReqAttributeException
from ..exceptions import MinosTypeAttributeException
from ..logs import (
    log,
)
from .types import MissingSentinel
from .types import ModelRef

PYTHON_INMUTABLE_TYPES = (str, int, bool, float, bytes)
PYTHON_LIST_TYPES = (list, tuple)
PYTHON_ARRAY_TYPES = (dict,)
PYTHON_NULL_TYPE = type(None)

T = t.TypeVar("T")


class ModelField:
    """Represents a model field."""

    __slots__ = "_name", "_type", "_value", "_validator"

    def __init__(
        self,
        name: str,
        type_val: t.Type[T],
        value: T = MissingSentinel,
        validator: t.Optional[t.Callable[[t.Any], bool]] = None,
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
    def _validator_function(self) -> t.Optional[t.Callable[[t.Any], T]]:
        if self.validator is None:
            return None
        if inspect.ismethod(self.validator):
            return self.validator.__func__

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

        value = self._cast_value(self._type, data)

        # Validation call will be here!
        if self.validator is not None and value is not None and not self.validator(value):
            raise MinosAttributeValidationException(self.name, value)

        self._value = value

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

        if type_field != type(None):
            if data is None:
                raise MinosReqAttributeException(f"'{self.name}' field is 'None'.")

            if data is MissingSentinel:
                raise MinosReqAttributeException(f"'{self.name}' field is missing.")

        raise MinosTypeAttributeException(
            f"The '{type_field}' type does not match with the given data type: {type(data)}"
        )

    def _cast_single_value(self, type_field: t.Type, data: t.Any) -> t.Any:
        if type_field is type(None):
            return self._cast_none_value(type_field, data)

        if type_field in PYTHON_INMUTABLE_TYPES:
            return self._cast_simple_value(type_field, data)

        return self._cast_composed_value(type_field, data)

    @staticmethod
    def _cast_none_value(type_field: t.Type, data: t.Any) -> t.Any:
        if data is None or data is MissingSentinel:
            log.debug("the Value passed is None")
            return None

        raise MinosTypeAttributeException(
            f"The '{type_field}' type does not match with the given data type: {type(data)}"
        )

    def _cast_simple_value(self, type_field: t.Type, data: t.Any) -> t.Any:
        if data is None:
            raise MinosReqAttributeException(f"'{self.name}' field is 'None'.")

        if data is MissingSentinel:
            raise MinosReqAttributeException(f"'{self.name}' field is missing.")

        if type_field is int and self._is_int(data):
            log.debug("the Value passed is an integer")
            return int(data)

        if type_field is bool and self._is_bool(data):
            log.debug("the Value passed is an integer")
            return data

        if type_field is str and self._is_string(data):
            log.debug("the Value passed is a string")
            return data

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

    def _cast_composed_value(self, type_field: t.Type, data: t.Any) -> t.Any:
        origin_type = t.get_origin(type_field)
        if origin_type is None:
            raise MinosMalformedAttributeException(f"'{self.name}' field is malformed. Type: '{type_field}'.")

        if data is None:
            raise MinosReqAttributeException(f"'{self.name}' field is 'None'.")

        if data is MissingSentinel:
            raise MinosReqAttributeException(f"'{self.name}' field is missing.")

        if origin_type is list:
            converted_data = self._convert_list(data, t.get_args(type_field)[0])
            if isinstance(converted_data, bool) and not converted_data:
                raise MinosTypeAttributeException(
                    f"{type(data)} could not be converted into {t.get_args(type_field)[0]} type"
                )
            return converted_data

        if origin_type is dict:
            converted_data = self._convert_dict(data, t.get_args(type_field)[0], t.get_args(type_field)[1])
            if isinstance(converted_data, bool) and not converted_data:
                raise MinosTypeAttributeException(
                    f"{type(data)} could not be converted into {type_field} key, value types"
                )
            return converted_data

        if origin_type is ModelRef:
            converted_data = self._convert_model_ref(data, t.get_args(type_field)[0])
            if isinstance(converted_data, bool) and not converted_data:
                raise MinosTypeAttributeException(
                    f"{type(data)} could not be converted into {t.get_args(type_field)[0]} type"
                )
            return converted_data

        raise MinosTypeAttributeException(
            f"The '{type_field}' type does not match with the given data type: {type(data)}"
        )

    def _convert_list(self, data: list, type_values: t.Any) -> t.Union[bool, list[t.Any]]:
        if not isinstance(data, list):
            return False

        data_converted = self._convert_list_params(data, type_values)

        if isinstance(data_converted, bool) and not data_converted:
            return False

        return data_converted

    def _convert_dict(self, data: list, type_keys: t.Type, type_values: t.Type) -> t.Union[bool, dict[t.Any, t.Any]]:
        if not isinstance(data, dict):
            return False

        data_converted = self._convert_dict_params(data, type_keys, type_values)

        if isinstance(data_converted, bool) and not data_converted:
            return False

        return data_converted

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
    def _convert_model_ref(data: t.Any, model_type: t.Type) -> t.Union[bool, t.Any]:
        if not isinstance(data, model_type):
            return False
        return data

    def _convert_list_params(self, data: t.Iterable, type_params: t.Type) -> t.Union[bool, t.Any]:
        """
        check if the parameters list are equal to @type_params type
        """
        converted = list()
        for item in data:
            try:
                value = self._cast_value(type_params, item)
                converted.append(value)
            except (MinosTypeAttributeException, MinosReqAttributeException):
                return False
        return converted

    @property
    def avro_schema(self) -> dict[str, t.Any]:
        """Compute the avro schema of the field.

        :return: A dictionary object.
        """
        return {"name": self.name, "type": self._build_schema(self._type)}

    def _build_schema(self, type_field: t.Type) -> t.Any:
        origin = t.get_origin(type_field)
        if origin is not t.Union:
            return self._build_single_schema(type_field)
        return self._build_union_schema(type_field)

    def _build_union_schema(self, type_field: t.Type) -> t.Any:
        ans = list()
        alternatives = t.get_args(type_field)
        for alternative_type in alternatives:
            step = self._build_single_schema(alternative_type)
            ans.append(step)
        return ans

    def _build_single_schema(self, type_field: t.Type) -> t.Any:
        if type_field is type(None):
            return self._build_none_schema(type_field)

        if type_field in PYTHON_INMUTABLE_TYPES:
            return self._build_simple_schema(type_field)

        return self._build_composed_schema(type_field)

    @staticmethod
    def _build_none_schema(type_field: t.Type) -> t.Any:
        return "null"

    @staticmethod
    def _build_simple_schema(type_field: t.Type) -> t.Any:
        if type_field is int:
            return "long"

        if type_field is bool:
            return "boolean"

        if type_field is str:
            return "string"

        raise Exception()

    def _build_composed_schema(self, type_field: t.Type) -> t.Any:
        origin_type = t.get_origin(type_field)
        if origin_type is None:
            raise MinosMalformedAttributeException(f"'{self.name}' field is malformed. Type: '{type_field}'.")

        if origin_type is list:
            return self._build_list_schema(type_field)

        if origin_type is dict:
            return self._build_dict_schema(type_field)

        if origin_type is ModelRef:
            return self._build_model_ref_schema(type_field)

        raise Exception

    def _build_list_schema(self, type_field: t.Type) -> dict[str, t.Any]:
        return {"type": "array", "items": self._build_schema(t.get_args(type_field)[0]), "default": []}

    def _build_dict_schema(self, type_field: t.Type) -> dict[str, t.Any]:
        return {"type": "map", "values": self._build_schema(t.get_args(type_field)[1]), "default": {}}

    @staticmethod
    def _build_model_ref_schema(type_field: t.Type) -> t.Union[bool, t.Any]:
        return t.get_args(type_field)[0].__name__

    @property
    def avro_data(self):
        """Compute the avro data of the model.

        :return: A dictionary object.
        """
        return self._to_avro_raw(self.value)

    def _to_avro_raw(self, value: t.Any) -> t.Any:
        if value is None:
            return "null"
        if type(value) in PYTHON_INMUTABLE_TYPES:
            return value
        if isinstance(value, list):
            return [self._to_avro_raw(v) for v in value]
        if isinstance(value, dict):
            return {k: self._to_avro_raw(v) for k, v in value.items()}
        return value.avro_data

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
        yield from (self.name, self.type, self.value, self._validator_function)

    def __repr__(self):
        return (
            f"ModelField(name={repr(self.name)}, type={repr(self.type)}, "
            f"value={repr(self.value)}, validator={self._validator_name})"
        )
