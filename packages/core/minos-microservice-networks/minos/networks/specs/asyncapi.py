from itertools import (
    chain,
)

from minos.common import (
    Config,
)

from ..decorators import (
    EnrouteCollector,
    enroute,
)
from ..requests import (
    Request,
    Response,
)


class AsyncAPIService:
    def __init__(self, config: Config):
        self.config = config
        self.spec = SPECIFICATION_SCHEMA.copy()

    @enroute.rest.command("/spec/asyncapi", "GET")
    def generate_specification(self, request: Request) -> Response:
        events = self.get_events()

        for event in events:
            topic: str = event["topic"]
            event_spec = {}

            self.spec["channels"][topic] = event_spec

        return Response(self.spec)

    def get_events(self) -> list[dict]:
        events = list()
        for name in self.config.get_services():
            decorators = EnrouteCollector(name, self.config).get_broker_event()
            events += [{"topic": decorator.topic} for decorator in set(chain(*decorators.values()))]

        return events


SPECIFICATION_SCHEMA = {
    "asyncapi": "2.3.0",
    "info": {"title": "", "version": ""},
    "channels": {},
}
