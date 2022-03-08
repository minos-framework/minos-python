import graphene

from minos.plugins.graphql_aiohttp.schema import (
    AllQuery,
)

from .abc import (
    GQLBaseHandler,
)


class GraphqlHandler(GQLBaseHandler):
    @property
    def schema(self):
        return graphene.Schema(query=AllQuery)
