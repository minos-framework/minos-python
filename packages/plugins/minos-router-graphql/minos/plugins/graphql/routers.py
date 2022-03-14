from __future__ import (
    annotations,
)

import json
from typing import (
    Callable,
)

from graphql import (
    GraphQLSchema,
    graphql,
    print_schema,
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


class GraphQlHttpRouter(HttpRouter):
    """TODO"""

    def __init__(self, schema: GraphQLSchema, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._schema = schema

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> GraphQlHttpRouter:
        builder = EnrouteBuilder(*config.services, middleware=config.middleware)
        routes = builder.get_all(config=config, **kwargs)

        from minos.plugins.graphql import (
            GraphQLSchemaBuilder,
        )

        schema = GraphQLSchemaBuilder.build(routes)

        return cls(schema)

    @property
    def routes(self) -> dict[HttpEnrouteDecorator, Callable]:
        """TODO"""
        return {
            HttpEnrouteDecorator("/graphql", "POST"): self.handle_post,
            HttpEnrouteDecorator("/graphql/schema", "GET"): self.handle_schema_get,
        }

    async def handle_post(self, request: HttpRequest) -> HttpResponse:
        """TODO"""
        status = 200
        result = await self.execute_graphql()

        if result and result.errors:
            status = 500

        errors = result.errors

        if errors is None:
            errors = []

        return HttpResponse(
            {"data": result.data, "errors": [err.message for err in errors]},
            status=status,
            headers={"Access-Control-Allow-Origin": "*"},
        )

    async def execute_graphql(self):
        graphql_req = await self.graphql_request
        context_value = graphql_req.get("context", {})
        variables = graphql_req.get("variables", {})

        # context_value["application"] = self.app
        source = graphql_req["query"]
        result = await graphql(
            schema=self._schema,
            source=source,
            context_value=context_value,
            variable_values=variables,
        )

        return result

    @property
    async def graphql_request(self):
        if self.request.method == "GET":
            return json.loads(self.request.query["q"])

        return await self.request.json()

    async def handle_schema_get(self, request: HttpRequest) -> HttpResponse:
        """TODO"""
        return HttpResponse(print_schema(self._schema))
