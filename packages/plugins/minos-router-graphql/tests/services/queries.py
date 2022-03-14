from graphql import (
    GraphQLField,
    GraphQLString,
)

from minos.networks import (
    Request,
    Response,
    enroute,
)


class QueryService:
    @enroute.graphql.query(name="order_query", argument=GraphQLString, output=GraphQLString)
    def get_order(self, request: Request):
        """For testng purposes."""

        return Response("eu38hj32-889283-j2jjb5kl")

    @enroute.graphql.query(name="ticket_query", argument=GraphQLString, output=GraphQLString)
    def get_ticket(self, request: Request):
        """For testng purposes."""

        return Response("zdw4gg4g-gser44gkl-jh4j3h4h")
