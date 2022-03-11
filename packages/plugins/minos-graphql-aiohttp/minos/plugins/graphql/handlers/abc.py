import json
import logging
import urllib

import aiohttp
from aiohttp import (
    web,
)
from graphql import (
    graphql,
)

from minos.plugins.graphql_aiohttp.star_wars_example import (
    star_wars_schema,
)


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
        )

        if result and result.errors:
            status = 500

        errors = result.errors

        if errors is None:
            errors = []

        return web.json_response(
            {"data": result.data, "errors": [err.message for err in errors]},
            status=status,
            headers={"Access-Control-Allow-Origin": "*"},
        )

    async def execute_graphql(self):
        graphql_req = await self.graphql_request
        context_value = graphql_req.get("context", {})
        variables = graphql_req.get("variables", {})

        context_value["application"] = self.app
        source = graphql_req["query"]
        result = await graphql(
            schema=star_wars_schema,
            source=source,
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
