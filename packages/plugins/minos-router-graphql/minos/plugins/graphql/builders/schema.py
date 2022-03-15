from __future__ import (
    annotations,
)

from functools import (
    wraps,
)
from inspect import (
    isawaitable,
)
from typing import (
    Any,
)

from graphql import (
    GraphQLArgument,
    GraphQLField,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
)

from minos.networks import (
    EnrouteDecoratorKind,
    InMemoryRequest,
)


class GraphQLSchemaBuilder:
    """TODO"""

    def __init__(self, *args, **kwargs):
        self.schema = GraphQLSchema(**kwargs)

    @classmethod
    def build(cls, routes) -> GraphQLSchema:
        """TODO"""
        schema_args = cls._build(routes)
        return cls(**schema_args).schema

    @classmethod
    def _build(cls, routes) -> dict:
        query = cls._build_queries(routes)
        mutation = cls._build_mutations(routes)

        return {"query": query, "mutation": mutation}

    @staticmethod
    def adapt_callback(callback):
        """TODO"""

        @wraps(callback)
        async def _wrapper(_source, _info, raw: Any = None):
            request = InMemoryRequest(raw)

            response = callback(request)
            if isawaitable(response):
                response = await response

            return await response.content()

        return _wrapper

    @classmethod
    def _build_queries(cls, routes):
        fields = dict()
        for route, callback in routes.items():
            callback = cls.adapt_callback(callback)
            if route.KIND == EnrouteDecoratorKind.Query:
                fields[route.name] = cls._build_field(route, callback)

        result = GraphQLObjectType(
            "Query", fields={"hello": GraphQLField(GraphQLString, resolve=lambda obj, info: "world")}
        )
        if len(fields) > 0:
            result = GraphQLObjectType("Query", fields=fields)

        return result

    @classmethod
    def _build_mutations(cls, routes):
        fields = dict()
        for route, callback in routes.items():
            callback = cls.adapt_callback(callback)

            if route.KIND == EnrouteDecoratorKind.Command:
                fields[route.name] = cls._build_field(route, callback)

        result = None
        if len(fields) > 0:
            result = GraphQLObjectType("Mutation", fields=fields)

        return result

    @staticmethod
    def _build_field(item, callback):
        args = None
        if item.argument is not None:
            args = {"request": GraphQLArgument(item.argument)}
        return GraphQLField(item.output, args=args, resolve=callback)
