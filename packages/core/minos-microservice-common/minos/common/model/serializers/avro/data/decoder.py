from __future__ import (
    annotations,
)

import logging
from collections.abc import (
    Iterable,
    Mapping,
)
from contextlib import (
    suppress,
)
from datetime import (
    date,
    datetime,
    time,
    timedelta,
    timezone,
)
from itertools import (
    zip_longest,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)
from uuid import (
    UUID,
)

from .....exceptions import (
    DataDecoderMalformedTypeException,
    DataDecoderRequiredValueException,
    DataDecoderTypeException,
)
from ....types import (
    MissingSentinel,
    ModelType,
    NoneType,
    TypeHintBuilder,
    is_model_subclass,
    is_type_subclass,
    unpack_typevar,
)
from ...abc import (
    DataDecoder,
)

if TYPE_CHECKING:
    from ....abc import (
        Model,
    )

logger = logging.getLogger(__name__)


class AvroDataDecoder(DataDecoder):
    """Avro Data Decoder class."""

    def __init__(self, type_: Optional[type] = None):
        self.type_ = type_

    def build(self, data: Any, type_: Any = MissingSentinel, **kwargs) -> Any:
        """Cast data type according to the field definition.

        :param data: Data to be casted.
        :param type_: The type of the decoded data.
        :return: The casted object.
        """
        if type_ is MissingSentinel:
            type_ = self.type_
        return self._build(type_, data, **kwargs)

    def _build(self, type_: type, data: Any, **kwargs) -> Any:
        origin = get_origin(type_)
        if origin is not Union:
            return self._build_single(type_, data, **kwargs)
        return self._build_union(type_, data, **kwargs)

    def _build_union(self, type_: type, data: Any, **kwargs) -> Any:
        alternatives = get_args(type_)
        for alternative_type in alternatives:
            with suppress(Exception):
                return self._build_single(alternative_type, data, **kwargs)

        if type_ is not NoneType:
            if data is None:
                raise DataDecoderRequiredValueException(f"Value is {None!r}.")

            if data is MissingSentinel:
                raise DataDecoderRequiredValueException("Value is missing.")

        raise DataDecoderTypeException(type_, data)

    def _build_single(self, type_: type, data: Any, **kwargs) -> Any:
        if type_ is Any:
            type_ = TypeHintBuilder(data).build()
        if isinstance(type_, TypeVar):
            unpacked_type = unpack_typevar(type_)
            return self._build(unpacked_type, data, **kwargs)

        if type_ is NoneType:
            return self._build_none(type_, data, **kwargs)

        if data is None:
            raise DataDecoderRequiredValueException(f"Value is {None!r}.")

        if data is MissingSentinel:
            raise DataDecoderRequiredValueException("Value is missing.")

        if is_model_subclass(type_):
            # noinspection PyTypeChecker
            return self._build_model(type_, data, **kwargs)

        if is_type_subclass(type_):
            if issubclass(type_, bool):
                return self._build_bool(data, **kwargs)

            if issubclass(type_, int):
                return self._build_int(type_, data, **kwargs)

            if issubclass(type_, float):
                return self._build_float(data, **kwargs)

            if issubclass(type_, str):
                return self._build_string(type_, data, **kwargs)

            if issubclass(type_, bytes):
                return self._build_bytes(data, **kwargs)

            if issubclass(type_, datetime):
                return self._build_datetime(data, **kwargs)

            if issubclass(type_, timedelta):
                return self._build_timedelta(data, **kwargs)

            if issubclass(type_, date):
                return self._build_date(data, **kwargs)

            if issubclass(type_, time):
                return self._build_time(data, **kwargs)

            if issubclass(type_, UUID):
                return self._build_uuid(data, **kwargs)

            if isinstance(type_, ModelType):
                return self._build_model_type(type_, data, **kwargs)

        return self._build_collection(type_, data, **kwargs)

    @staticmethod
    def _build_none(type_: type, data: Any, **kwargs) -> Any:
        if data is None or data is MissingSentinel:
            return None

        raise DataDecoderTypeException(type_, data)

    @staticmethod
    def _build_int(type_, data: Any, **kwargs) -> int:
        try:
            return type_(data)
        except (ValueError, TypeError):
            raise DataDecoderTypeException(type_, data)

    @staticmethod
    def _build_float(data: Any, **kwargs) -> float:
        try:
            return float(data)
        except (ValueError, TypeError):
            raise DataDecoderTypeException(float, data)

    @staticmethod
    def _build_bool(data: Any, **kwargs) -> bool:
        if not isinstance(data, bool):
            raise DataDecoderTypeException(bool, data)
        return data

    @staticmethod
    def _build_string(type_, data: Any, **kwargs) -> str:
        if not isinstance(data, str):
            raise DataDecoderTypeException(str, data)
        return type_(data)

    @staticmethod
    def _build_bytes(data: Any, **kwargs) -> bytes:
        if not isinstance(data, bytes):
            raise DataDecoderTypeException(bytes, data)
        return data

    @staticmethod
    def _build_date(data: Any, **kwargs) -> date:
        if isinstance(data, date):
            return data
        elif isinstance(data, int):
            return date(1970, 1, 1) + timedelta(days=data)
        raise DataDecoderTypeException(date, data)

    @staticmethod
    def _build_time(data: Any, **kwargs) -> time:
        if isinstance(data, time):
            return data
        if isinstance(data, int):
            return (datetime(1, 1, 1) + timedelta(microseconds=data)).time()
        raise DataDecoderTypeException(time, data)

    @staticmethod
    def _build_datetime(data: Any, **kwargs) -> datetime:
        if isinstance(data, datetime):
            return data
        if isinstance(data, int):
            return datetime(1970, 1, 1, tzinfo=timezone.utc) + data * timedelta(microseconds=1)
        raise DataDecoderTypeException(datetime, data)

    @staticmethod
    def _build_timedelta(data: Any, **kwargs) -> timedelta:
        if isinstance(data, timedelta):
            return data
        if isinstance(data, int):
            return timedelta(microseconds=data)
        raise DataDecoderTypeException(timedelta, data)

    @staticmethod
    def _build_uuid(data: Any, **kwargs) -> UUID:
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

    def _build_model(self, type_: Type[Model], data: Any, **kwargs) -> Any:
        if is_type_subclass(type_) and isinstance(data, type_):
            return data
        return self._build_model_type(ModelType.from_model(type_), data, **kwargs)

    def _build_model_type(self, type_: ModelType, data: Any, **kwargs) -> Any:
        if hasattr(data, "model_type"):
            if ModelType.from_model(data) >= type_:
                return data

        if (ans := type_.model_cls.decode_data(self, data, type_, **kwargs)) is not MissingSentinel:
            return ans

        if isinstance(data, dict):
            with suppress(Exception):
                decoded_data = {
                    field_name: self._build(field_type, data[field_name], **kwargs)
                    for field_name, field_type in type_.type_hints.items()
                    if field_name in data
                }
                return type_(**decoded_data, additional_type_hints=type_.type_hints)

        with suppress(Exception):
            decoded_data = (
                self._build(field_type, field_value, **kwargs)
                for field_value, field_type in zip_longest((data,), type_.type_hints.values())
            )
            return type_(*decoded_data, additional_type_hints=type_.type_hints)

        raise DataDecoderTypeException(type_, data)

    def _build_collection(self, type_: type, data: Any, **kwargs) -> Any:
        origin_type = get_origin(type_)
        if origin_type is None:
            raise DataDecoderMalformedTypeException(f"Type is malformed. Obtained: '{type_}'.")

        if origin_type is list:
            return self._build_list(data, type_, **kwargs)

        if origin_type is set:
            return self._build_set(data, type_, **kwargs)

        if origin_type is dict:
            return self._build_dict(data, type_, **kwargs)

        raise DataDecoderTypeException(type_, data)

    def _build_list(self, data: Any, type_values: Any, **kwargs) -> list[Any]:
        type_values = get_args(type_values)[0]
        if isinstance(data, str) or not isinstance(data, Iterable):
            raise DataDecoderTypeException(list, data)

        return list(self._build_iterable(data, type_values, **kwargs))

    def _build_set(self, data: Any, type_values: Any, **kwargs) -> set[Any]:
        type_values = get_args(type_values)[0]
        if isinstance(data, str) or not isinstance(data, Iterable):
            raise DataDecoderTypeException(set, data)

        return set(self._build_iterable(data, type_values, **kwargs))

    def _build_dict(self, data: dict, type_: type, **kwargs) -> dict[str, Any]:
        type_keys, type_values = get_args(type_)
        if not isinstance(data, Mapping):
            raise DataDecoderTypeException(dict, data)

        if type_keys is not str:
            raise DataDecoderMalformedTypeException(f"dictionary keys must be {str!r}. Obtained: {type_keys!r}")

        keys = self._build_iterable(data.keys(), type_keys, **kwargs)
        values = self._build_iterable(data.values(), type_values, **kwargs)
        return dict(zip(keys, values))

    def _build_iterable(self, data: Iterable, type_params: type, **kwargs) -> Iterable:
        return (self._build(type_params, item, **kwargs) for item in data)
