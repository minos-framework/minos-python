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
    timezone,
)
from typing import (
    Any,
    Iterable,
    TypeVar,
    Union,
    get_args,
    get_origin,
)
from uuid import (
    UUID,
)

from ...exceptions import (
    DataDecoderMalformedTypeException,
    DataDecoderRequiredValueException,
    DataDecoderTypeException,
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

logger = logging.getLogger(__name__)


class AvroDataDecoder:
    """Avro Data Decoder class."""

    def __init__(self, type_: type):
        self.type_ = type_

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
            except (DataDecoderTypeException, DataDecoderRequiredValueException):
                pass

        if type_ is not NoneType:
            if data is None:
                raise DataDecoderRequiredValueException(f"Value is {None!r}.")

            if data is MissingSentinel:
                raise DataDecoderRequiredValueException("Value is missing.")

        raise DataDecoderTypeException(type_, data)

    def _cast_single_value(self, type_: type, data: Any) -> Any:
        if isinstance(type_, TypeVar):
            unpacked_type = unpack_typevar(type_)
            return self._cast_value(unpacked_type, data)

        if type_ is NoneType:
            return self._cast_none_value(type_, data)

        if data is None:
            raise DataDecoderRequiredValueException(f"Value is {None!r}.")

        if data is MissingSentinel:
            raise DataDecoderRequiredValueException("Value is missing.")

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

    @staticmethod
    def _cast_none_value(type_: type, data: Any) -> Any:
        if data is None or data is MissingSentinel:
            return None

        raise DataDecoderTypeException(type_, data)

    @staticmethod
    def _cast_int(type_, data: Any) -> int:
        try:
            return type_(data)
        except (ValueError, TypeError):
            raise DataDecoderTypeException(type_, data)

    @staticmethod
    def _cast_float(data: Any) -> float:
        try:
            return float(data)
        except (ValueError, TypeError):
            raise DataDecoderTypeException(float, data)

    @staticmethod
    def _cast_bool(data: Any) -> bool:
        if not isinstance(data, bool):
            raise DataDecoderTypeException(bool, data)
        return data

    @staticmethod
    def _cast_string(type_, data: Any) -> str:
        if not isinstance(data, str):
            raise DataDecoderTypeException(str, data)
        return type_(data)

    @staticmethod
    def _cast_bytes(data: Any) -> bytes:
        if not isinstance(data, bytes):
            raise DataDecoderTypeException(bytes, data)
        return data

    @staticmethod
    def _cast_date(data: Any) -> date:
        if isinstance(data, date):
            return data
        elif isinstance(data, int):
            return date(1970, 1, 1) + timedelta(days=data)
        raise DataDecoderTypeException(date, data)

    @staticmethod
    def _cast_time(data: Any) -> time:
        if isinstance(data, time):
            return data
        if isinstance(data, int):
            return (datetime(1, 1, 1) + timedelta(microseconds=data)).time()
        raise DataDecoderTypeException(time, data)

    @staticmethod
    def _cast_datetime(data: Any) -> datetime:
        if isinstance(data, datetime):
            return data
        if isinstance(data, int):
            return datetime(1970, 1, 1, tzinfo=timezone.utc) + data * timedelta(microseconds=1)
        raise DataDecoderTypeException(datetime, data)

    @staticmethod
    def _cast_uuid(data: Any) -> UUID:
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
        raise DataDecoderTypeException(UUID, data)

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
            if ModelType.from_model(data) >= type_:
                return data

        raise DataDecoderTypeException(type_, data)

    def _cast_composed_value(self, type_: type, data: Any) -> Any:
        origin_type = get_origin(type_)
        if origin_type is None:
            raise DataDecoderMalformedTypeException(f"Type is malformed. Obtained: '{type_}'.")

        if origin_type is list:
            return self._cast_list(data, type_)

        if origin_type is dict:
            return self._cast_dict(data, type_)

        if origin_type is ModelRef:
            return self._cast_model_ref(data, type_)

        raise DataDecoderTypeException(type_, data)

    def _cast_list(self, data: list, type_values: Any) -> list[Any]:
        type_values = get_args(type_values)[0]
        if not isinstance(data, list):
            raise DataDecoderTypeException(list, data)

        return list(self._cast_iterable(data, type_values))

    def _cast_dict(self, data: dict, type_: type) -> dict[str, Any]:
        type_keys, type_values = get_args(type_)
        if not isinstance(data, dict):
            raise DataDecoderTypeException(dict, data)

        if type_keys is not str:
            raise DataDecoderMalformedTypeException(f"dictionary keys must be {str!r}. Obtained: {type_keys!r}")

        keys = self._cast_iterable(data.keys(), type_keys)
        values = self._cast_iterable(data.values(), type_values)
        return dict(zip(keys, values))

    def _cast_iterable(self, data: Iterable, type_params: type) -> Iterable:
        return (self._cast_value(type_params, item) for item in data)

    def _cast_model_ref(self, data: Any, type_: type) -> Any:
        inner_type = get_args(type_)[0]
        if not is_aggregate_type(inner_type):
            raise DataDecoderMalformedTypeException(
                f"'ModelRef[T]' T type must follow the 'Aggregate' protocol. Obtained: {inner_type!r}"
            )

        return self._cast_value(Union[inner_type, UUID], data)
