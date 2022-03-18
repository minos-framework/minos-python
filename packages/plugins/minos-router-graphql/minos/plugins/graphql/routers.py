from __future__ import (
    annotations,
)

from typing import (
    Callable,
)

from minos.networks import (
    EnrouteDecorator,
    HttpEnrouteDecorator,
    HttpRouter,
)

from .builders import (
    GraphQLSchemaBuilder,
)
from .decorators import (
    GraphQlEnrouteDecorator,
)
from .handlers import (
    GraphQlHandler,
)


class GraphQlHttpRouter(HttpRouter):
    """GraphQl Http Router class."""

    def _filter_routes(self, routes: dict[EnrouteDecorator, Callable]) -> dict[EnrouteDecorator, Callable]:
        routes = {
            decorator: callback
            for decorator, callback in routes.items()
            if isinstance(decorator, GraphQlEnrouteDecorator)
        }
        schema = GraphQLSchemaBuilder.build(routes)
        handler = GraphQlHandler(schema)
        return {
            HttpEnrouteDecorator("/graphql", "POST"): handler.execute_operation,
            HttpEnrouteDecorator("/graphql/schema", "GET"): handler.get_schema,
        }
