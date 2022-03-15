from graphql import (
    GraphQLSchema,
    graphql,
    print_schema,
)

from minos.networks import (
    HttpRequest,
    HttpResponse,
)


class GraphQlHandler:
    """TODO"""

    def __init__(self, schema: GraphQLSchema):
        self._schema = schema

    async def execute_operation(self, request: HttpRequest) -> HttpResponse:
        """TODO"""

        content = await request.content()

        source = dict()
        variables = dict()

        if type(content) == str:
            source = content

        if type(content) == dict:
            if "query" in content:
                source = content["query"]

            if "variables" in content:
                variables = content["variables"]

        result = await graphql(schema=self._schema, source=source, variable_values=variables)

        errors = result.errors
        if errors is None:
            errors = list()

        if len(errors):
            status = 400
        else:
            status = 200

        return HttpResponse({"data": result.data, "errors": [err.message for err in errors]}, status=status)

    async def get_schema(self, request: HttpRequest) -> HttpResponse:
        """TODO"""
        return HttpResponse(print_schema(self._schema))
