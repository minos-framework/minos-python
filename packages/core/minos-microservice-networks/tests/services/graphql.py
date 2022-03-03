from minos.networks import (
    Request,
    Response,
    enroute,
)


class GraphqlService:
    @enroute.graphql(url="/graphql", method="POST")
    def add_ticket(self, request: Request) -> Response:
        return Response({
          "data": {},
          "errors": []
        })
