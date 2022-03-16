from graphql import (
    GraphQLSchema,
    graphql,
    print_schema,
)

from minos.networks import (
    HttpRequest,
    HttpResponse,
    ResponseException,
)


class GraphQlHandler:
    """GraphQl Handler"""

    def __init__(self, schema: GraphQLSchema):
        self._schema = schema

    async def execute_operation(self, request: HttpRequest) -> HttpResponse:
        """Execute incoming request extracting variables and passing to graphql"""

        content = await request.content()

        source = dict()
        variables = dict()

        if isinstance(content, str):
            source = content

        if isinstance(content, dict):
            if "query" in content:
                source = content["query"]

            if "variables" in content:
                variables = content["variables"]

        result = await graphql(schema=self._schema, source=source, variable_values=variables)

        errors = result.errors
        if errors is None:
            errors = list()

        status = 200

        if len(errors):
            status = 500
            for error in errors:
                if isinstance(error.original_error, ResponseException):
                    status = error.original_error.status

        return HttpResponse({"data": result.data, "errors": [err.message for err in errors]}, status=status)

    async def get_schema(self, request: HttpRequest) -> HttpResponse:
        """Get schema"""
        return HttpResponse(print_schema(self._schema))
