from graphql import GraphQLField, GraphQLString
from minos.networks import (
    Request,
    Response,
    enroute,
)


class QueryService:
    @enroute.graphql.query(name="order-query", argument=GraphQLField(GraphQLString), output=GraphQLString)
    def get_order(self, request: Request):
        """For testng purposes."""

        return "eu38hj32-889283-j2jjb5kl"

    @enroute.graphql.query(name="ticket-query", argument=GraphQLField(GraphQLString), output=GraphQLString)
    def get_ticket(self, request: Request):
        """For testng purposes."""

        return "zdw4gg4g-gser44gkl-jh4j3h4h"
