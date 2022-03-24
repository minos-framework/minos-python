import traceback
from typing import (
    Any,
)

from graphql import (
    ExecutionResult,
    GraphQLSchema,
    graphql,
    print_schema,
)

from minos.networks import (
    Request,
    Response,
    ResponseException,
)
import logging

logger = logging.getLogger(__name__)


class GraphQlHandler:
    """GraphQl Handler"""

    def __init__(self, schema: GraphQLSchema):
        self._schema = schema

    async def execute_operation(self, request: Request) -> Response:
        """Execute incoming request extracting variables and passing to graphql.

        :param request: The request containing the graphql operation.
        :return: A response containing the graphql result.
        """
        arguments = await self._build_graphql_arguments(request)
        result = await graphql(schema=self._schema, **arguments)

        return self._build_response_from_graphql(result)

    @staticmethod
    async def _build_graphql_arguments(request: Request) -> dict[str, Any]:
        content = await request.content()
        source = dict()
        variables = dict()

        if isinstance(content, str):
            source = content
        elif isinstance(content, dict):
            if "query" in content:
                source = content["query"]
            if "variables" in content:
                variables = content["variables"]

        return {"source": source, "variable_values": variables}

    @staticmethod
    def _build_response_from_graphql(result: ExecutionResult) -> Response:
        errors = result.errors
        if errors is None:
            errors = list()

        status = 200

        if len(errors):
            status = 500
            for error in errors:
                if isinstance(error.original_error, ResponseException):
                    status = error.original_error.status
                    error_trace = ''.join(traceback.format_tb(error.original_error.__traceback__))
                    logger.exception(f"Raised a system exception:\n {error_trace}")
                else:
                    logger.error(f"Raised an application exception:\n {error}")

        content = {"data": result.data, "errors": [err.message for err in errors]}

        return Response(content, status=status)

    async def get_schema(self, request: Request) -> Response:
        """Get schema

        :param request: An empty request.
        :return: A Response containing the schema as a string.
        """
        return Response(print_schema(self._schema))
