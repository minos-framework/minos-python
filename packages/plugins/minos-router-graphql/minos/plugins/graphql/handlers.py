from graphql import (
    graphql,
    print_schema,
)

from minos.networks import (
    HttpRequest,
    HttpResponse,
)


class GraphQlHandler:
    """TODO"""

    def __init__(self, schema):
        self._schema = schema

    async def execute_operation(self, request: HttpRequest) -> HttpResponse:
        """TODO"""

        source = await request.content()

        result = await graphql(schema=self._schema, source=source)
        errors = result.errors
        if errors is None:
            errors = list()

        if len(errors):
            status = 500
        else:
            status = 200

        return HttpResponse({"data": result.data, "errors": [err.message for err in errors]}, status=status)

    async def get_schema(self, request: HttpRequest) -> HttpResponse:
        """TODO"""
        return HttpResponse(print_schema(self._schema))
