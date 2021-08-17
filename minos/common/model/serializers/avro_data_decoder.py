"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from collections.abc import (
    MutableSet,
)
from datetime import (
    date,
    datetime,
    time,
    timedelta,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Mapping,
    TypeVar,
    Union,
    get_args,
    get_origin,
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
    MissingSentinel,
    ModelRef,
    ModelType,
    NoneType,
    TypeHintBuilder,
    is_aggregate_type,
    is_model_subclass,
    is_type_subclass,
    unpack_typevar,
)

if TYPE_CHECKING:
    from ..fields import (
        Field,
    )
logger = logging.getLogger(__name__)


class AvroDataDecoder:
    """Avro Data Decoder class."""

    def __init__(self, name: str, type_: type):
        self.name = name
        self.type_ = type_

    @classmethod
    def from_field(cls, field: Field) -> AvroDataDecoder:
        """Build a new instance from a ``Field``.

        :param field: The model field.
        :return: A new avro schema builder instance.
        """
        return cls(field.name, field.type)

    def build(self, data: Any) -> Any:
        """Cast data type according to the field definition..

        :param data: Data to be casted.
        :return: Casted object.
        """
        return self._cast_value(self.type_, data)

    def _cast_value(self, type_: type, data: Any) -> Any:
        if type_ is Any:
            type_ = TypeHintBuilder(data).build()
        origin = get_origin(type_)
        if origin is not Union:
            return self._cast_single_value(type_, data)
        return self._cast_union_value(type_, data)

    def _cast_union_value(self, type_: type, data: Any) -> Any:
        alternatives = get_args(type_)
        for alternative_type in alternatives:
            try:
                return self._cast_single_value(alternative_type, data)
            except (MinosTypeAttributeException, MinosReqAttributeException):
                pass

        if type_ is not NoneType:
            if data is None:
                raise MinosReqAttributeException(f"{self.name!r} field is {None!r}.")

            if data is MissingSentinel:
                raise MinosReqAttributeException(f"{self.name!r} field is missing.")

        raise MinosTypeAttributeException(self.name, type_, data)

    def _cast_single_value(self, type_: type, data: Any) -> Any:
        if isinstance(type_, TypeVar):
            unpacked_type = unpack_typevar(type_)
            return self._cast_value(unpacked_type, data)

        if type_ is NoneType:
            return self._cast_none_value(type_, data)

        if data is None:
            raise MinosReqAttributeException(f"{self.name!r} field is '{None!r}'.")

        if data is MissingSentinel:
            raise MinosReqAttributeException(f"{self.name!r} field is missing.")

        if is_type_subclass(type_):
            if issubclass(type_, bool):
                return self._cast_bool(data)

            if issubclass(type_, int):
                return self._cast_int(type_, data)

            if issubclass(type_, float):
                return self._cast_float(data)

            if issubclass(type_, str):
                return self._cast_string(type_, data)

            if issubclass(type_, bytes):
                return self._cast_bytes(data)

            if issubclass(type_, datetime):
                return self._cast_datetime(data)

            if issubclass(type_, date):
                return self._cast_date(data)

            if issubclass(type_, time):
                return self._cast_time(data)

            if issubclass(type_, UUID):
                return self._cast_uuid(data)

            if isinstance(type_, ModelType):
                return self._cast_model_type(type_, data)

        if is_model_subclass(type_):
            return self._cast_model(type_, data)

        return self._cast_composed_value(type_, data)

    def _cast_none_value(self, type_: type, data: Any) -> Any:
        if data is None or data is MissingSentinel:
            return None

        raise MinosTypeAttributeException(self.name, type_, data)

    def _cast_int(self, type_, data: Any) -> int:
        try:
            return type_(data)
        except (ValueError, TypeError):
            raise MinosTypeAttributeException(self.name, type_, data)

    def _cast_float(self, data: Any) -> float:
        try:
            return float(data)
        except (ValueError, TypeError):
            raise MinosTypeAttributeException(self.name, float, data)

    def _cast_bool(self, data: Any) -> bool:
        if not isinstance(data, bool):
            raise MinosTypeAttributeException(self.name, bool, data)
        return data

    def _cast_string(self, type_, data: Any) -> str:
        if not isinstance(data, str):
            raise MinosTypeAttributeException(self.name, str, data)
        return type_(data)

    def _cast_bytes(self, data: Any) -> bytes:
        if not isinstance(data, bytes):
            raise MinosTypeAttributeException(self.name, bytes, data)
        return data

    def _cast_date(self, data: Any) -> date:
        if isinstance(data, date):
            return data
        elif isinstance(data, int):
            return date(1970, 1, 1) + timedelta(days=data)
        raise MinosTypeAttributeException(self.name, date, data)

    def _cast_time(self, data: Any) -> time:
        if isinstance(data, time):
            return data
        if isinstance(data, int):
            return (datetime(1, 1, 1) + timedelta(microseconds=data)).time()
        raise MinosTypeAttributeException(self.name, time, data)

    def _cast_datetime(self, data: Any) -> datetime:
        if isinstance(data, datetime):
            return data
        if isinstance(data, int):
            return datetime(1970, 1, 1) + data * timedelta(microseconds=1)
        raise MinosTypeAttributeException(self.name, datetime, data)

    def _cast_uuid(self, data: Any) -> UUID:
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
        raise MinosTypeAttributeException(self.name, UUID, data)

    def _cast_model(self, type_: type, data: Any) -> Any:
        if is_type_subclass(type_) and isinstance(data, type_):
            return data
        # noinspection PyUnresolvedReferences
        return self._cast_model_type(ModelType.from_model(type_), data)

    def _cast_model_type(self, type_: ModelType, data: Any) -> Any:
        if isinstance(data, dict):
            data |= {k: self._cast_value(v, data.get(k, None)) for k, v in type_.type_hints.items()}
            return type_(**data)

        if hasattr(data, "model_type"):
            if isinstance(data, MutableSet) and isinstance(data, type_.model_cls) and not len(data):
                return data
            if ModelType.from_model(data) == type_:
                return data

        raise MinosTypeAttributeException(self.name, type_, data)

    def _cast_composed_value(self, type_: type, data: Any) -> Any:
        origin_type = get_origin(type_)
        if origin_type is None:
            raise MinosMalformedAttributeException(f"{self.name!r} field is malformed. Type: '{type_}'.")

        if origin_type is list:
            return self._convert_list(data, type_)

        if origin_type is dict:
            return self._convert_dict(data, type_)

        if origin_type is ModelRef:
            return self._convert_model_ref(data, type_)

        raise MinosTypeAttributeException(self.name, type_, data)

    def _convert_list(self, data: list, type_values: Any) -> list[Any]:
        type_values = get_args(type_values)[0]
        if not isinstance(data, list):
            raise MinosTypeAttributeException(self.name, list, data)

        return self._convert_list_params(data, type_values)

    def _convert_dict(self, data: list, type_: type) -> dict[str, Any]:
        type_keys, type_values = get_args(type_)
        if not isinstance(data, dict):
            raise MinosTypeAttributeException(self.name, dict, data)

        if type_keys is not str:
            raise MinosMalformedAttributeException(f"dictionary keys must be {str!r}. Obtained: {type_keys!r}")

        return self._convert_dict_params(data, type_keys, type_values)

    def _convert_dict_params(self, data: Mapping, type_keys: type, type_values: type) -> dict[Any, Any]:
        keys = self._convert_list_params(data.keys(), type_keys)
        values = self._convert_list_params(data.values(), type_values)
        return dict(zip(keys, values))

    def _convert_model_ref(self, data: Any, type_: type) -> Any:
        inner_type = get_args(type_)[0]
        if not is_aggregate_type(inner_type):
            raise MinosMalformedAttributeException(
                f"'ModelRef[T]' T type must follow the 'Aggregate' protocol. Obtained: {inner_type!r}"
            )

        return self._cast_value(Union[inner_type, UUID], data)

    def _convert_list_params(self, data: Iterable, type_params: type) -> list[Any]:
        """
        check if the parameters list are equal to @type_params type
        """
        converted = list()
        for item in data:
            value = self._cast_value(type_params, item)
            converted.append(value)
        return converted
