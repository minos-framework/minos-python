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
from typing import (
    TYPE_CHECKING,
    Any,
    Type,
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
    ModelType,
    NoneType,
    TypeHintBuilder,
    is_model_subclass,
    is_type_subclass,
    unpack_typevar,
)

if TYPE_CHECKING:
    from ..abc import (
        Model,
    )

logger = logging.getLogger(__name__)


class AvroDataDecoder:
    """Avro Data Decoder class."""

    def __init__(self, type_: type = None):
        self.type_ = type_

    def build(self, data: Any, type_: Any = MissingSentinel) -> Any:
        """Cast data type according to the field definition.

        :param data: Data to be casted.
        :param type_: The type of the decoded data.
        :return: The casted object.
        """
        if type_ is MissingSentinel:
            type_ = self.type_
        return self._build(type_, data)

    def _build(self, type_: type, data: Any) -> Any:
        origin = get_origin(type_)
        if origin is not Union:
            return self._build_single(type_, data)
        return self._build_union(type_, data)

    def _build_union(self, type_: type, data: Any) -> Any:
        alternatives = get_args(type_)
        for alternative_type in alternatives:
            with suppress(Exception):
                return self._build_single(alternative_type, data)

        if type_ is not NoneType:
            if data is None:
                raise DataDecoderRequiredValueException(f"Value is {None!r}.")

            if data is MissingSentinel:
                raise DataDecoderRequiredValueException("Value is missing.")

        raise DataDecoderTypeException(type_, data)

    def _build_single(self, type_: type, data: Any) -> Any:
        if type_ is Any:
            type_ = TypeHintBuilder(data).build()
        if isinstance(type_, TypeVar):
            unpacked_type = unpack_typevar(type_)
            return self._build(unpacked_type, data)

        if type_ is NoneType:
            return self._build_none(type_, data)

        if data is None:
            raise DataDecoderRequiredValueException(f"Value is {None!r}.")

        if data is MissingSentinel:
            raise DataDecoderRequiredValueException("Value is missing.")

        if is_model_subclass(type_):
            # noinspection PyTypeChecker
            return self._build_model(type_, data)

        if is_type_subclass(type_):
            if issubclass(type_, bool):
                return self._build_bool(data)

            if issubclass(type_, int):
                return self._build_int(type_, data)

            if issubclass(type_, float):
                return self._build_float(data)

            if issubclass(type_, str):
                return self._build_string(type_, data)

            if issubclass(type_, bytes):
                return self._build_bytes(data)

            if issubclass(type_, datetime):
                return self._build_datetime(data)

            if issubclass(type_, timedelta):
                return self._build_timedelta(data)

            if issubclass(type_, date):
                return self._build_date(data)

            if issubclass(type_, time):
                return self._build_time(data)

            if issubclass(type_, UUID):
                return self._build_uuid(data)

            if isinstance(type_, ModelType):
                return self._build_model_type(type_, data)

        return self._build_collection(type_, data)

    @staticmethod
    def _build_none(type_: type, data: Any) -> Any:
        if data is None or data is MissingSentinel:
            return None

        raise DataDecoderTypeException(type_, data)

    @staticmethod
    def _build_int(type_, data: Any) -> int:
        try:
            return type_(data)
        except (ValueError, TypeError):
            raise DataDecoderTypeException(type_, data)

    @staticmethod
    def _build_float(data: Any) -> float:
        try:
            return float(data)
        except (ValueError, TypeError):
            raise DataDecoderTypeException(float, data)

    @staticmethod
    def _build_bool(data: Any) -> bool:
        if not isinstance(data, bool):
            raise DataDecoderTypeException(bool, data)
        return data

    @staticmethod
    def _build_string(type_, data: Any) -> str:
        if not isinstance(data, str):
            raise DataDecoderTypeException(str, data)
        return type_(data)

    @staticmethod
    def _build_bytes(data: Any) -> bytes:
        if not isinstance(data, bytes):
            raise DataDecoderTypeException(bytes, data)
        return data

    @staticmethod
    def _build_date(data: Any) -> date:
        if isinstance(data, date):
            return data
        elif isinstance(data, int):
            return date(1970, 1, 1) + timedelta(days=data)
        raise DataDecoderTypeException(date, data)

    @staticmethod
    def _build_time(data: Any) -> time:
        if isinstance(data, time):
            return data
        if isinstance(data, int):
            return (datetime(1, 1, 1) + timedelta(microseconds=data)).time()
        raise DataDecoderTypeException(time, data)

    @staticmethod
    def _build_datetime(data: Any) -> datetime:
        if isinstance(data, datetime):
            return data
        if isinstance(data, int):
            return datetime(1970, 1, 1, tzinfo=timezone.utc) + data * timedelta(microseconds=1)
        raise DataDecoderTypeException(datetime, data)

    @staticmethod
    def _build_timedelta(data: Any) -> timedelta:
        if isinstance(data, timedelta):
            return data
        if isinstance(data, int):
            return timedelta(microseconds=data)
        raise DataDecoderTypeException(timedelta, data)

    @staticmethod
    def _build_uuid(data: Any) -> UUID:
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

    def _build_model(self, type_: Type[Model], data: Any) -> Any:
        if is_type_subclass(type_) and isinstance(data, type_):
            return data
        return type_.decode_data(self, data, ModelType.from_model(type_))

    def _build_model_type(self, type_: ModelType, data: Any) -> Any:
        if hasattr(data, "model_type"):
            if ModelType.from_model(data) >= type_:
                return data

        if isinstance(data, dict):
            with suppress(Exception):
                decoded_data = {n: self._build(t, data.get(n, None)) for n, t in type_.type_hints.items()}
                return type_(**decoded_data, additional_type_hints=type_.type_hints)

        with suppress(Exception):
            # TODO: check if the condition is needed.
            decoded_data = data if isinstance(data, (list, tuple)) else (data,)
            decoded_data = (self._build(t, d) for d, t in zip(decoded_data, type_.type_hints.values()))
            return type_(*decoded_data, additional_type_hints=type_.type_hints)

        raise DataDecoderTypeException(type_, data)

    def _build_collection(self, type_: type, data: Any) -> Any:
        origin_type = get_origin(type_)
        if origin_type is None:
            raise DataDecoderMalformedTypeException(f"Type is malformed. Obtained: '{type_}'.")

        if origin_type is list:
            return self._build_list(data, type_)

        if origin_type is set:
            return self._build_set(data, type_)

        if origin_type is dict:
            return self._build_dict(data, type_)

        raise DataDecoderTypeException(type_, data)

    def _build_list(self, data: Any, type_values: Any) -> list[Any]:
        type_values = get_args(type_values)[0]
        if isinstance(data, str) or not isinstance(data, Iterable):
            raise DataDecoderTypeException(list, data)

        return list(self._build_iterable(data, type_values))

    def _build_set(self, data: Any, type_values: Any) -> set[Any]:
        type_values = get_args(type_values)[0]
        if isinstance(data, str) or not isinstance(data, Iterable):
            raise DataDecoderTypeException(set, data)

        return set(self._build_iterable(data, type_values))

    def _build_dict(self, data: dict, type_: type) -> dict[str, Any]:
        type_keys, type_values = get_args(type_)
        if not isinstance(data, Mapping):
            raise DataDecoderTypeException(dict, data)

        if type_keys is not str:
            raise DataDecoderMalformedTypeException(f"dictionary keys must be {str!r}. Obtained: {type_keys!r}")

        keys = self._build_iterable(data.keys(), type_keys)
        values = self._build_iterable(data.values(), type_values)
        return dict(zip(keys, values))

    def _build_iterable(self, data: Iterable, type_params: type) -> Iterable:
        return (self._build(type_params, item) for item in data)
