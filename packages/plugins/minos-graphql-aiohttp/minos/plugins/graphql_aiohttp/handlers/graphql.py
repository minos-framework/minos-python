import graphene

from .abc import GQLBaseHandler
from minos.plugins.graphql_aiohttp.schema import AllQuery


class GraphqlHandler(GQLBaseHandler):
    @property
    def schema(self):
        return graphene.Schema(query=AllQuery)
