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
    Type,
    Union,
    get_args,
    get_origin,
)
from uuid import (
    UUID,
)

from graphql import (
    GraphQLBoolean,
    GraphQLEnumType,
    GraphQLFloat,
    GraphQLInputObjectType,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLString,
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
            ans.append(step)
        return tuple(ans)

    def _build_single(self, type_: type, **kwargs) -> Any:
        if type_ is Any:
            pass  # FIXME: This is a design decision that must be revisited in the future.

        if is_type_subclass(type_):
            if issubclass(type_, NoneType):
                # FIXME: This is a design decision that must be revisited in the future.
                return GraphQLBoolean

            if issubclass(type_, bool):
                return GraphQLNonNull(GraphQLBoolean)

            if issubclass(type_, int):
                return GraphQLNonNull(GraphQLInt)

            if issubclass(type_, float):
                return GraphQLNonNull(GraphQLFloat)

            if issubclass(type_, str):
                return GraphQLNonNull(GraphQLString)

            if issubclass(type_, bytes):
                return GraphQLNonNull(GraphQLString)

            if issubclass(type_, datetime):
                return GraphQLNonNull(GraphQLString)

            if issubclass(type_, timedelta):
                return GraphQLNonNull(GraphQLString)

            if issubclass(type_, date):
                return GraphQLNonNull(GraphQLString)

            if issubclass(type_, time):
                return GraphQLNonNull(GraphQLString)

            if issubclass(type_, UUID):
                return GraphQLNonNull(GraphQLString)

            if isinstance(type_, ModelType):
                return self._build_model_type(type_, **kwargs)

        if isinstance(type_, Model) or is_model_subclass(type_):
            # noinspection PyTypeChecker
            return self._build_model(type_, **kwargs)

        return self._build_collection(type_, **kwargs)

    @staticmethod
    def _build_enum(type_: type[Enum], **kwargs):
        return GraphQLNonNull(GraphQLEnumType(type_.__name__, type_))

    def _build_model(self, type_: Type[Model], **kwargs) -> Any:
        raw = ModelType.from_model(type_)
        return self._build_model_type(raw, **kwargs)

    def _build_model_type(self, type_: ModelType, is_input: bool = False, **kwargs) -> Any:
        if is_input:
            cls = GraphQLInputObjectType
        else:
            cls = GraphQLObjectType
        return GraphQLNonNull(cls(type_.name, {n: self._build(t, **kwargs) for n, t in type_.type_hints.items()}))

    def _build_collection(self, type_: type, **kwargs) -> Any:
        origin_type = get_origin(type_)

        if origin_type is list:
            return self._build_list(type_, **kwargs)

        if origin_type is set:
            return self._build_set(type_, **kwargs)

        if origin_type is dict:
            return self._build_dict(type_, **kwargs)

        raise ValueError(f"Given field type is not supported: {type_}")  # pragma: no cover

    def _build_set(self, type_: type, **kwargs):
        return self._build_list(type_, **kwargs)

    def _build_list(self, type_: type, **kwargs):
        return GraphQLNonNull(GraphQLList(self._build(get_args(type_)[0], **kwargs)))

    def _build_dict(self, type_: type, is_input: bool = False, **kwargs):
        if is_input:
            cls = GraphQLInputObjectType
        else:
            cls = GraphQLObjectType

        item = GraphQLNonNull(
            cls(
                "DictItem",
                {
                    "key": self._build(get_args(type_)[0], **kwargs),
                    "value": self._build(get_args(type_)[1], **kwargs),
                },
            )
        )

        return GraphQLNonNull(GraphQLList(item))
