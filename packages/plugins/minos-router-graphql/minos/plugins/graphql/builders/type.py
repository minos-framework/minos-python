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

from graphql import (
    GraphQLBoolean,
    GraphQLFloat,
    GraphQLID,
    GraphQLInt,
    GraphQLList,
    GraphQLObjectType,
    GraphQLString, GraphQLEnumType,
    GraphQLNonNull,
)

from minos.common import (
    MissingSentinel,
    Model,
    ModelType,
    NoneType,
    SchemaEncoder,
)
from minos.common.model.types import (
    is_model_subclass,
    is_type_subclass,
)

logger = logging.getLogger(__name__)


class GraphQlSchemaEncoder(SchemaEncoder):
    """GraphQl Schema Encoder class."""

    def __init__(self, type_: type = None):
        self.type_ = type_

    def build(self, type_=MissingSentinel, **kwargs):
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
            return fasdfsa

        if is_type_subclass(type_):
            if issubclass(type_, NoneType):
                return fasdfsa

            if issubclass(type_, bool):
                return GraphQLBoolean

            if issubclass(type_, int):
                return GraphQLInt

            if issubclass(type_, float):
                return GraphQLFloat

            if issubclass(type_, str):
                return GraphQLString

            if issubclass(type_, bytes):
                return fasdfsa

            if issubclass(type_, datetime):
                return fasdfsa

            if issubclass(type_, timedelta):
                return fasdfsa

            if issubclass(type_, date):
                return fasdfsa

            if issubclass(type_, time):
                return fasdfsa

            if issubclass(type_, UUID):
                return GraphQLID

            if isinstance(type_, ModelType):
                return self._build_model_type(type_, **kwargs)

        if isinstance(type_, Model) or is_model_subclass(type_):
            # noinspection PyTypeChecker
            return self._build_model(type_, **kwargs)

        return self._build_collection(type_, **kwargs)

    def _build_enum(self, type_: type[Enum], **kwargs):
        return GraphQLEnumType(type_.__name__, type_)

    def _build_model(self, type_: Type[Model], **kwargs) -> Any:
        raw = ModelType.from_model(type_)
        return [self._build_model_type(raw, **kwargs)]

    def _build_model_type(self, type_: ModelType, **kwargs) -> Any:
        if (ans := type_.model_cls.encode_schema(self, type_, **kwargs)) is not MissingSentinel:
            return ans

        schema = GraphQLObjectType(
            type_.name,
            {n: self._build(t, **kwargs) for n, t in type_.type_hints.items()}
        )
        return schema

    @classmethod
    def _patch_namespace(cls, namespace: Optional[str]) -> Optional[str]:
        if len(namespace) > 0:
            namespace += f".{cls.generate_random_str()}"
        return namespace

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
        return self._build_list(type_, **kwargs)

    def _build_list(self, type_: type, **kwargs) -> dict[str, Any]:
        return GraphQLList(self._build(get_args(type_)[0], **kwargs))

    def _build_dict(self, type_: type, **kwargs) -> dict[str, Any]:
        return fasdfsa

    @staticmethod
    def generate_random_str() -> str:
        """Generate a random string

        :return: A random string value.
        """
        return str(uuid4())
