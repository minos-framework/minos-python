from graphql.utilities import (
    print_schema
)

from .abc import GQLBaseHandler
from minos.plugins.graphql_aiohttp.star_wars_example import (
    star_wars_schema,
)

class GraphqlHandler(GQLBaseHandler):

    @property
    def schema(self):
        return print_schema(star_wars_schema)
