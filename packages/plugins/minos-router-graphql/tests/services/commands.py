from graphql import GraphQLField, GraphQLString
from minos.networks import (
    BrokerResponse,
    Request,
    Response,
    enroute,
)


class CommandService:
    @enroute.graphql.command(name="order-command", argument=GraphQLField(GraphQLString), output=GraphQLString)
    def create_order(self, request: Request):
        """For testng purposes."""

        return "eu38hj32-889283-j2jjb5kl"

    @enroute.graphql.command(name="ticket-command", argument=GraphQLField(GraphQLString), output=GraphQLString)
    def create_ticket(self, request: Request):
        """For testng purposes."""

        return "zdw4gg4g-gser44gkl-jh4j3h4h"
