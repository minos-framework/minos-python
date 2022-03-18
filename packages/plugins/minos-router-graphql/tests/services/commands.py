from graphql import (
    GraphQLString,
)

from minos.networks import (
    Request,
    Response,
    enroute,
)


class CommandService:
    @enroute.graphql.command(name="order_command", argument=GraphQLString, output=GraphQLString)
    def create_order(self, request: Request):
        """For testng purposes."""

        return Response("eu38hj32-889283-j2jjb5kl")

    @enroute.graphql.command(name="ticket_command", argument=GraphQLString, output=GraphQLString)
    def create_ticket(self, request: Request):
        """For testng purposes."""

        return Response("zdw4gg4g-gser44gkl-jh4j3h4h")
