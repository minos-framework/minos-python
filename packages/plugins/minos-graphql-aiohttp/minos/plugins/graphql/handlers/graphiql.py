import aiohttp_jinja2

from .abc import (
    BaseHandler,
)


class GraphiqlHandler(BaseHandler):
    @aiohttp_jinja2.template("graphiql.html")
    async def get(self):
        return {"base_url": f"/graphql"}
