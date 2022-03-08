import asyncio
import json
import logging
import urllib

import aiohttp
from aiohttp import (
    web,
)

# from graphql.execution.executors.asyncio import AsyncioExecutor


class BaseHandler(web.View):
    @property
    def fetch(self):
        return self.app.fetch

    @property
    def app(self):
        return self.request.app

    async def options(self):
        return web.Response(
            status=204,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, PATCH",
                "Access-Control-Allow-Headers": "x-requested-with,access-control-allow-origin,authorization,content-type",
            },
        )


class GQLBaseHandler(BaseHandler):
    async def get(self):
        return await self.post()

    async def post(self):
        return await self.handle_graqhql()

    async def handle_graqhql(self):
        status = 200
        result = await self.execute_graphql()
        logging.debug(
            "GraphQL result data: %s errors: %s",
            result.data,
            result.errors,
            # result.invalid,
        )

        if result and result.errors:
            status = 500

            # ex = ExecutionError(errors=result.errors)
            # logging.debug('GraphQL Error: %s', ex)

        errors = result.errors

        if errors is None:
            errors = []

        return web.json_response(
            {"data": result.data, "errors": [str(err) for err in errors]},
            status=status,
            headers={"Access-Control-Allow-Origin": "*"},
        )

    async def execute_graphql(self):
        graphql_req = await self.graphql_request
        logging.debug("graphql request: %s", graphql_req)
        context_value = graphql_req.get("context", {})
        variables = graphql_req.get("variables", {})

        context_value["application"] = self.app

        # executor = AsyncioExecutor(loop=asyncio.get_event_loop())
        result = self.schema.execute(
            graphql_req["query"],
            # executor=executor,
            # return_promise=True,
            context_value=context_value,
            variable_values=variables,
        )

        ref = self.request.headers.get("referer")
        url_path = ""

        if ref:
            url = urllib.parse.urlparse(ref)
            url_path = url.path

        if result.errors:
            if "/graphiql" not in url_path:
                aiohttp.log.server_logger.error(f'Graphql query error: for query "{graphql_req}"')

        return result

    @property
    async def graphql_request(self):
        if self.request.method == "GET":
            return json.loads(self.request.query["q"])

        return await self.request.json()
