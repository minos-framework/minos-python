from __future__ import (
    annotations,
)

from collections.abc import (
    Callable,
)
from functools import (
    wraps,
)
from inspect import (
    isawaitable,
)
from typing import (
    Any,
    Awaitable,
    Optional,
    Union, get_type_hints, get_args,
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
    Request,
    Response,
)
from .type import GraphQlSchemaEncoder

from ..decorators import (
    GraphQlEnrouteDecorator,
)


class GraphQLSchemaBuilder:
    """TODO"""

    def __init__(self, *args, **kwargs):
        self.schema = GraphQLSchema(**kwargs)

    @classmethod
    def build(cls, routes: dict[GraphQlEnrouteDecorator, Callable]) -> GraphQLSchema:
        """TODO"""
        schema_args = cls._build(routes)
        return cls(**schema_args).schema

    @classmethod
    def _build(cls, routes: dict[GraphQlEnrouteDecorator, Callable]) -> dict[str, Optional[GraphQLObjectType]]:
        query = cls._build_queries(routes)
        mutation = cls._build_mutations(routes)

        return {"query": query, "mutation": mutation}

    @staticmethod
    def adapt_callback(
        callback: Callable[[Request], Union[Optional[Response], Awaitable[Optional[Response]]]]
    ) -> Callable[[Any, Any, Any], Awaitable[Any]]:
        """TODO"""

        @wraps(callback)
        async def _wrapper(_source, _info, request: Any = None):
            request = InMemoryRequest(request)

            response = callback(request)
            if isawaitable(response):
                response = await response

            return await response.content()

        return _wrapper

    @classmethod
    def _build_queries(cls, routes: dict[GraphQlEnrouteDecorator, Callable]) -> GraphQLObjectType:
        fields = dict()
        for route, callback in routes.items():
            if route.KIND == EnrouteDecoratorKind.Query:
                fields[route.name] = cls._build_field(route, callback)

        if not len(fields):
            fields["dummy"] = GraphQLField(
                type_=GraphQLString,
                description="Dummy query added to surpass the 'Type Query must define at least one field' constraint."
            )

        return GraphQLObjectType("Query", fields=fields)

    @classmethod
    def _build_mutations(cls, routes: dict[GraphQlEnrouteDecorator, Callable]) -> Optional[GraphQLObjectType]:
        fields = dict()
        for route, callback in routes.items():

            if route.KIND == EnrouteDecoratorKind.Command:
                fields[route.name] = cls._build_field(route, callback)

        if not len(fields):
            return None

        return GraphQLObjectType("Mutation", fields=fields)

    @classmethod
    def _build_field(cls, item: GraphQlEnrouteDecorator, callback: Callable) -> GraphQLField:

        if item.output is not None:
            argument = item.argument
            output = item.output
        else:
            hints = get_type_hints(callback)
            assert len(hints) == 2
            response_hint = hints.pop("return")
            request_hint = hints.popitem()[1]

            argument_hint = get_args(request_hint)[0]
            output_hint = get_args(response_hint)[0]

            argument = GraphQlSchemaEncoder().build(argument_hint)
            output = GraphQlSchemaEncoder().build(output_hint)

            print("done")

        args = None
        if argument is not None:
            args = {"request": GraphQLArgument(argument)}

        callback = cls.adapt_callback(callback)

        return GraphQLField(output, args=args, resolve=callback)
