from __future__ import (
    annotations,
)

from typing import (
    Callable,
)

from graphql import GraphQLSchema
from minos.common import (
    MinosConfig,
)
from minos.networks import (
    EnrouteBuilder,
    HttpEnrouteDecorator,
    HttpRequest,
    HttpResponse,
    HttpRouter,
    EnrouteDecoratorKind,
)


class GraphQlHttpRouter(HttpRouter):
    """TODO"""

    def __init__(self, schema: GraphQLSchema, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._schema = schema

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> GraphQlHttpRouter:
        builder = EnrouteBuilder(*config.services, middleware=config.middleware)
        routes = builder.get_all(config=config, **kwargs)

        queries = []
        mutations = []
        for route in routes:
            if route.KIND == EnrouteDecoratorKind.Query:
                queries.append(route)

            if route.KIND == EnrouteDecoratorKind.Command:
                mutations.append(route)
        from minos.plugins.graphql import GraphQLSchemaBuilder
        schema = GraphQLSchemaBuilder.build(queries=queries, mutations=mutations)

        return cls(schema)

    @property
    def routes(self) -> dict[HttpEnrouteDecorator, Callable]:
        """TODO"""
        return {
            HttpEnrouteDecorator("/graphql", "POST"): self.handle_post,
            HttpEnrouteDecorator("/graphql", "GET"): self.handle_get,
        }

    async def handle_post(self, request: HttpRequest) -> HttpResponse:
        """TODO"""
        graphql_query = await request.content()

        content = await self._schema(graphql_query)

        return HttpResponse(content)

    async def handle_get(self, request: HttpRequest) -> HttpResponse:
        """TODO"""
        content = self._schema.json()
        return HttpResponse(content)
