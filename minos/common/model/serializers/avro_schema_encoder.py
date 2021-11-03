from __future__ import (
    annotations,
)

import logging
from datetime import (
    date,
    datetime,
    time,
    timedelta,
)
from typing import (
    Any,
    Optional,
    Union,
    get_args,
    get_origin,
)
from uuid import (
    UUID,
    uuid4,
)

from ..types import (
    ModelType,
    NoneType,
    is_model_subclass,
    is_type_subclass,
)
from .constants import (
    AVRO_ARRAY,
    AVRO_BOOLEAN,
    AVRO_BYTES,
    AVRO_DATE,
    AVRO_DOUBLE,
    AVRO_INT,
    AVRO_MAP,
    AVRO_NULL,
    AVRO_SET,
    AVRO_STRING,
    AVRO_TIME,
    AVRO_TIMEDELTA,
    AVRO_TIMESTAMP,
    AVRO_UUID,
)

logger = logging.getLogger(__name__)


class AvroSchemaEncoder:
    """Avro Schema Encoder class."""

    def __init__(self, type_: type, name: Optional[str] = None):
        self.type_ = type_
        self.name = name

    def build(self) -> Union[dict, list, str]:
        """Build the avro schema for the given field.

        :return: A dictionary object.
        """
        type_ = self._build_schema(self.type_)

        if self.name is None:
            return type_
        return {"name": self.name, "type": type_}

    def _build_schema(self, type_: type) -> Any:
        origin = get_origin(type_)
        if origin is not Union:
            return self._build_single_schema(type_)
        return self._build_union_schema(type_)

    def _build_union_schema(self, type_: type) -> Any:
        ans = list()
        alternatives = get_args(type_)
        for alternative_type in alternatives:
            step = self._build_single_schema(alternative_type)
            if isinstance(step, list):
                ans += step
            else:
                ans.append(step)
        return ans

    def _build_single_schema(self, type_: type) -> Any:
        if type_ is Any:
            # FIXME: This is a design decision that must be revisited in the future.
            return AVRO_NULL

        if is_type_subclass(type_):
            if issubclass(type_, NoneType):
                return AVRO_NULL

            if issubclass(type_, bool):
                return AVRO_BOOLEAN

            if issubclass(type_, int):
                return AVRO_INT

            if issubclass(type_, float):
                return AVRO_DOUBLE

            if issubclass(type_, str):
                return AVRO_STRING

            if issubclass(type_, bytes):
                return AVRO_BYTES

            if issubclass(type_, datetime):
                return AVRO_TIMESTAMP

            if issubclass(type_, timedelta):
                return AVRO_TIMEDELTA

            if issubclass(type_, date):
                return AVRO_DATE

            if issubclass(type_, time):
                return AVRO_TIME

            if issubclass(type_, UUID):
                return AVRO_UUID

            if isinstance(type_, ModelType):
                return self._build_model_type_schema(type_)

        if is_model_subclass(type_):
            return self._build_model_schema(type_)

        return self._build_composed_schema(type_)

    def _build_model_schema(self, type_: type) -> Any:
        return [self._build_model_type_schema(ModelType.from_model(type_))]

    def _build_model_type_schema(self, type_: ModelType) -> Any:
        namespace = type_.namespace
        if len(namespace) > 0:
            namespace = f"{type_.namespace}.{self.generate_random_str()}"
        schema = {
            "name": type_.name,
            "namespace": namespace,
            "type": "record",
            "fields": [type(self)(name=k, type_=v).build() for k, v in type_.type_hints.items()],
        }
        return schema

    def _build_composed_schema(self, type_: type) -> Any:
        origin_type = get_origin(type_)

        if origin_type is list:
            return self._build_list_schema(type_)

        if origin_type is set:
            return self._build_set_schema(type_)

        if origin_type is dict:
            return self._build_dict_schema(type_)

        raise ValueError(f"Given field type is not supported: {type_}")  # pragma: no cover

    def _build_set_schema(self, type_: type) -> dict[str, Any]:
        schema = self._build_list_schema(type_)
        return schema | AVRO_SET

    def _build_list_schema(self, type_: type) -> dict[str, Any]:
        return {"type": AVRO_ARRAY, "items": self._build_schema(get_args(type_)[0])}

    def _build_dict_schema(self, type_: type) -> dict[str, Any]:
        return {"type": AVRO_MAP, "values": self._build_schema(get_args(type_)[1])}

    @staticmethod
    def generate_random_str() -> str:
        """Generate a random string

        :return: A random string value.
        """
        return str(uuid4())
