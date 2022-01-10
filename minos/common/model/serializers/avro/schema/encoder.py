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
from enum import (
    Enum,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Type,
    Union,
    get_args,
    get_origin,
)
from uuid import (
    UUID,
    uuid4,
)

from .....importlib import (
    classname,
)
from ....types import (
    FieldType,
    MissingSentinel,
    ModelType,
    NoneType,
    is_model_subclass,
    is_type_subclass,
)
from ...abc import (
    SchemaEncoder,
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

if TYPE_CHECKING:
    from ....abc import (
        Model,
    )
    from ....fields import (
        Field,
    )

logger = logging.getLogger(__name__)


class AvroSchemaEncoder(SchemaEncoder):
    """Avro Schema Encoder class."""

    def __init__(self, type_: type = None):
        self.type_ = type_

    def build(self, type_=MissingSentinel, **kwargs) -> Union[dict, list, str]:
        """Build the avro schema for the given field.

        :param type_: The type to be encoded as a schema.
        :return: A dictionary object.
        """
        if type_ is MissingSentinel:
            type_ = self.type_
        return self._build(type_, **kwargs)

    def _build(self, type_, **kwargs) -> Any:
        if get_origin(type_) is Union:
            return self._build_union(type_, **kwargs)

        if is_type_subclass(type_) and issubclass(type_, Enum):
            return self._build_enum(type_, **kwargs)

        return self._build_single(type_, **kwargs)

    def _build_union(self, type_: type, **kwargs) -> Any:
        ans = list()
        alternatives = get_args(type_)
        for alternative_type in alternatives:
            step = self._build(alternative_type, **kwargs)
            if isinstance(step, list):
                ans += step
            else:
                ans.append(step)
        return ans

    def _build_single(self, type_: type, **kwargs) -> Any:
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
                return self._build_model_type(type_, **kwargs)

        from ....abc import (
            Model,
        )

        if isinstance(type_, Model) or is_model_subclass(type_):
            # noinspection PyTypeChecker
            return self._build_model(type_, **kwargs)

        from ....fields import (
            Field,
        )

        if isinstance(type_, Field):
            return self._build_field(type_, **kwargs)

        return self._build_collection(type_, **kwargs)

    def _build_enum(self, type_: type, **kwargs):
        return {"type": self._build_single(type_, **kwargs), "logicalType": classname(type_)}

    def _build_model(self, type_: Type[Model], **kwargs) -> Any:
        raw = ModelType.from_model(type_)
        return [self._build_model_type(raw, **kwargs)]

    def _build_model_type(self, type_: ModelType, **kwargs) -> Any:
        if (ans := type_.model_cls.encode_schema(self, type_, **kwargs)) is not MissingSentinel:
            return ans

        schema = {
            "name": type_.name,
            "namespace": self._patch_namespace(type_.namespace),
            "type": "record",
            "fields": [self._build_field(FieldType(n, t), **kwargs) for n, t in type_.type_hints.items()],
        }
        return schema

    @classmethod
    def _patch_namespace(cls, namespace: Optional[str]) -> Optional[str]:
        if len(namespace) > 0:
            namespace += f".{cls.generate_random_str()}"
        return namespace

    def _build_field(self, field: Union[Field, FieldType], **kwargs):
        return {"name": field.name, "type": self._build(field.type, **kwargs)}

    def _build_collection(self, type_: type, **kwargs) -> Any:
        origin_type = get_origin(type_)

        if origin_type is list:
            return self._build_list(type_, **kwargs)

        if origin_type is set:
            return self._build_set(type_, **kwargs)

        if origin_type is dict:
            return self._build_dict(type_, **kwargs)

        raise ValueError(f"Given field type is not supported: {type_}")  # pragma: no cover

    def _build_set(self, type_: type, **kwargs) -> dict[str, Any]:
        schema = self._build_list(type_, **kwargs)
        return schema | AVRO_SET

    def _build_list(self, type_: type, **kwargs) -> dict[str, Any]:
        return {"type": AVRO_ARRAY, "items": self._build(get_args(type_)[0], **kwargs)}

    def _build_dict(self, type_: type, **kwargs) -> dict[str, Any]:
        return {"type": AVRO_MAP, "values": self._build(get_args(type_)[1], **kwargs)}

    @staticmethod
    def generate_random_str() -> str:
        """Generate a random string

        :return: A random string value.
        """
        return str(uuid4())
