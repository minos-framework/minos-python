"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
import typing as t
from datetime import (
    date,
    datetime,
    time,
    timedelta,
)
from uuid import (
    UUID,
)

from ...exceptions import (
    MinosMalformedAttributeException,
    MinosReqAttributeException,
    MinosTypeAttributeException,
)
from ..types import (
    PYTHON_IMMUTABLE_TYPES,
    MissingSentinel,
    ModelRef,
    ModelType,
)
from .utils import (
    _is_aggregate_cls,
    _is_model_cls,
)

if t.TYPE_CHECKING:
    from .fields import ModelField  # pragma: no cover
logger = logging.getLogger(__name__)

T = t.TypeVar("T")


class AvroDataDecoder:
    """Avro Data Decoder class."""

    def __init__(self, field_name: str, field_type: t.Type):
        self._name = field_name
        self._type = field_type

    @classmethod
    def from_field(cls, field: ModelField) -> AvroDataDecoder:
        """Build a new instance from a ``ModelField``.

        :param field: The model field.
        :return: A new avro schema builder instance.
        """
        return cls(field.name, field.type)

    def build(self, data: t.Any) -> t.Any:
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

        if type_field is not type(None):  # noqa: E721
            if data is None:
                raise MinosReqAttributeException(f"{self._name!r} field is {None!r}.")

            if data is MissingSentinel:
                raise MinosReqAttributeException(f"{self._name!r} field is missing.")

        raise MinosTypeAttributeException(self._name, type_field, data)

    def _cast_single_value(self, type_field: t.Type, data: t.Any) -> t.Any:
        if type_field is type(None):  # noqa: E721
            return self._cast_none_value(type_field, data)

        if type_field in PYTHON_IMMUTABLE_TYPES:
            return self._cast_simple_value(type_field, data)

        if type_field is date:
            return self._cast_date(data)

        if type_field is time:
            return self._cast_time(data)

        if type_field is datetime:
            return self._cast_datetime(data)

        if type_field is UUID:
            return self._cast_uuid(data)

        if isinstance(type_field, ModelType):
            return self._cast_model_type(type_field, data)

        if _is_model_cls(type_field):
            return self._cast_model(type_field, data)

        return self._cast_composed_value(type_field, data)

    def _cast_none_value(self, type_field: t.Type, data: t.Any) -> t.Any:
        if data is None or data is MissingSentinel:
            return None

        raise MinosTypeAttributeException(self._name, type_field, data)

    def _cast_simple_value(self, type_field: t.Type, data: t.Any) -> t.Any:
        if data is None:
            raise MinosReqAttributeException(f"{self._name!r} field is '{None!r}'.")

        if data is MissingSentinel:
            raise MinosReqAttributeException(f"{self._name!r} field is missing.")

        if type_field is int:
            return self._cast_int(data)

        if type_field is float:
            return self._cast_float(data)

        if type_field is bool:
            return self._cast_bool(data)

        if type_field is str:
            return self._cast_string(data)

        if type_field is bytes:
            return self._cast_bytes(data)

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

    def _cast_bytes(self, data: t.Any) -> bytes:
        if not isinstance(data, bytes):
            raise MinosTypeAttributeException(self._name, bytes, data)
        return data

    def _cast_date(self, data: t.Any) -> date:
        if isinstance(data, date):
            return data
        elif isinstance(data, int):
            return date(1970, 1, 1) + timedelta(days=data)
        raise MinosTypeAttributeException(self._name, date, data)

    def _cast_time(self, data: t.Any) -> time:
        if isinstance(data, time):
            return data
        if isinstance(data, int):
            return (datetime(1, 1, 1) + timedelta(microseconds=data)).time()
        raise MinosTypeAttributeException(self._name, time, data)

    def _cast_datetime(self, data: t.Any) -> datetime:
        if isinstance(data, datetime):
            return data
        if isinstance(data, int):
            return datetime(1970, 1, 1) + data * timedelta(microseconds=1)
        raise MinosTypeAttributeException(self._name, datetime, data)

    def _cast_uuid(self, data: t.Any) -> UUID:
        if isinstance(data, UUID):
            return data
        elif isinstance(data, str):
            try:
                return UUID(hex=data)
            except ValueError:
                pass
        elif isinstance(data, bytes):
            try:
                return UUID(bytes=data)
            except ValueError:
                pass
        raise MinosTypeAttributeException(self._name, UUID, data)

    def _cast_model_type(self, type_field: ModelType, data: t.Any) -> t.Any:
        if isinstance(data, dict):
            data = {k: self._cast_value(v, data[k]) for k, v in type_field.type_hints.items()}
            return type_field(**data)

        if hasattr(data, "model_type") and type_field == data.model_type:
            return data

        raise MinosTypeAttributeException(self._name, type_field, data)

    def _cast_model(self, type_field: t.Type, data: t.Any) -> t.Any:
        if not isinstance(data, type_field):
            raise MinosTypeAttributeException(self._name, type_field, data)
        return data

    def _cast_composed_value(self, type_field: t.Type, data: t.Any) -> t.Any:
        origin_type = t.get_origin(type_field)
        if origin_type is None:
            raise MinosMalformedAttributeException(f"{self._name!r} field is malformed. Type: '{type_field}'.")

        if data is None:
            raise MinosReqAttributeException(f"{self._name!r} field is 'None'.")

        if data is MissingSentinel:
            raise MinosReqAttributeException(f"{self._name!r} field is missing.")

        if origin_type is list:
            return self._convert_list(data, type_field)

        if origin_type is dict:
            return self._convert_dict(data, type_field)

        if origin_type is ModelRef:
            return self._convert_model_ref(data, type_field)

        raise MinosTypeAttributeException(self._name, type_field, data)

    def _convert_list(self, data: list, type_values: t.Any) -> list[t.Any]:
        type_values = t.get_args(type_values)[0]
        if not isinstance(data, list):
            raise MinosTypeAttributeException(self._name, list, data)

        return self._convert_list_params(data, type_values)

    def _convert_dict(self, data: list, type_field: t.Type) -> dict[str, t.Any]:
        type_keys, type_values = t.get_args(type_field)
        if not isinstance(data, dict):
            raise MinosTypeAttributeException(self._name, dict, data)

        if type_keys is not str:
            raise MinosMalformedAttributeException(f"dictionary keys must be {str!r}. Obtained: {type_keys!r}")

        return self._convert_dict_params(data, type_keys, type_values)

    def _convert_dict_params(self, data: t.Mapping, type_keys: t.Type, type_values: t.Type) -> dict[t.Any, t.Any]:
        keys = self._convert_list_params(data.keys(), type_keys)
        values = self._convert_list_params(data.values(), type_values)
        return dict(zip(keys, values))

    def _convert_model_ref(self, data: t.Any, type_field: t.Type) -> t.Any:
        inner_type = t.get_args(type_field)[0]
        if not _is_aggregate_cls(inner_type):
            raise MinosMalformedAttributeException(
                f"'ModelRef[T]' T type must be a descendant of 'Aggregate'. Obtained: {inner_type!r}"
            )

        return self._cast_value(t.Union[inner_type, int], data)

    def _convert_list_params(self, data: t.Iterable, type_params: t.Type) -> list[t.Any]:
        """
        check if the parameters list are equal to @type_params type
        """
        converted = list()
        for item in data:
            value = self._cast_value(type_params, item)
            converted.append(value)
        return converted
