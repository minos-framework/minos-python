from __future__ import (
    annotations,
)

from typing import (
    Callable,
)

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    EnrouteBuilder,
    HttpEnrouteDecorator,
    HttpRequest,
    HttpResponse,
    HttpRouter,
)

from .decorators import (
    GraphQlEnrouteDecorator,
)


class GraphQlHttpRouter(HttpRouter):
    """TODO"""

    def __init__(self, schema, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._schema = schema

    @staticmethod
    def _from_config(config: MinosConfig, **kwargs) -> dict[(str, str), Callable]:
        builder = EnrouteBuilder(*config.services, middleware=config.middleware)
        routes = builder.get_all(config=config, **kwargs)

        graphql_routes = {
            GraphQlEnrouteDecorator(
                "CreateProduct", request=GraphQLField(GraphQLObjectType), response=GraphQLField(GraphQLObjectType)
            ): "fn1",
            GraphQlEnrouteDecorator(
                "UpdateProduct", request=GraphQLField(GraphQLObjectType), response=GraphQLField(GraphQLObjectType)
            ): "fn2",
        }

        schema = GraphQLSchema(
            query=GraphQLObjectType(
                name="RootQueryType",
                fields={
                    "CreateProduct": GraphQLField(
                        GraphQLString, args=GraphQLField(GraphQLObjectType), response=GraphQLField(GraphQLObjectType)
                    ),
                    "UpdateProduct": GraphQLField(
                        GraphQLString, args=GraphQLField(GraphQLObjectType), response=GraphQLField(GraphQLObjectType)
                    ),
                },
            )
        )

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
