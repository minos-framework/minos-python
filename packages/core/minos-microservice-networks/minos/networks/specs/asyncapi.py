from itertools import (
    chain,
)

from minos.common import (
    Config,
)

from ..decorators import (
    EnrouteAnalyzer,
    enroute,
)
from ..requests import (
    Request,
    Response,
)


class AsyncAPIService:
    """TODO"""

    def __init__(self, config: Config):
        self.config = config
        self.spec = BASE_SPEC.copy()

    @enroute.rest.command("/spec/asyncapi", "GET")
    def generate_spec(self, request: Request) -> Response:
        """TODO"""

        events = self.get_events()

        for event in events:
            topic: str = event["topic"]
            event_spec = {"publish": {"operationId": None, "message": None}}

            self.spec["channels"][topic] = event_spec

        return Response(self.spec)

    def get_events(self) -> list[dict]:
        """TODO"""

        events = list()
        for name in self.config.services:
            decorators = EnrouteAnalyzer(name, self.config).get_broker_event()
            events += [{"topic": decorator.topic} for decorator in set(chain(*decorators.values()))]

        return events


BASE_SPEC = {
    "asyncapi": "2.0.0",
    "info": {"title": None, "version": None},
    "description": None,
    "license": "MIT",
    "channels": {},
}
