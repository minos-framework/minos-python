"""This module contains the implementation of the graphql handler."""

import logging
import traceback
from typing import (
    Any,
)

from graphql import (
    ExecutionResult,
    GraphQLError,
    GraphQLSchema,
    graphql,
    print_schema,
)

from minos.networks import (
    Request,
    Response,
    ResponseException,
)

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

    def _build_response_from_graphql(self, result: ExecutionResult) -> Response:
        content = {"data": result.data}
        if result.errors is not None:
            content["errors"] = [err.message for err in result.errors]
            self._log_errors(result.errors)

        status = self._get_status(result)

        return Response(content, status=status)

    @staticmethod
    def _get_status(result: ExecutionResult) -> int:
        status = 200
        for error in result.errors or []:
            if error.original_error is None:
                current = 400
            elif isinstance(error.original_error, ResponseException):
                current = error.original_error.status
            else:
                current = 500
            status = max(status, current)
        return status

    @staticmethod
    def _log_errors(errors: list[GraphQLError]) -> None:
        for error in errors:
            if error.original_error is None:
                tb = repr(error)
            else:
                tb = "".join(traceback.format_tb(error.__traceback__))

            if error.original_error is None or isinstance(error.original_error, ResponseException):
                logger.error(f"Raised an application exception:\n {tb}")
            else:
                logger.exception(f"Raised a system exception:\n {tb}")

    async def get_schema(self, request: Request) -> Response:
        """Get schema

        :param request: An empty request.
        :return: A Response containing the schema as a string.
        """
        return Response(print_schema(self._schema))
