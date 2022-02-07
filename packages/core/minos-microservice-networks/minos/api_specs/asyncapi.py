from itertools import chain
from pathlib import Path

from minos.common import MinosConfig
from ..networks import enroute, EnrouteAnalyzer


class OpenAPIService:
    CONFIG_FILE_PATH = Path(__file__).parent / "config.yml"

    def __init__(self):
        self.spec = BASE_SPEC.copy()

    @enroute.rest.command("/spec/openapi")
    def generate_spec(self) -> dict:
        events = self.get_events()

        for event in events:
            topic: str = event["topic"]
            event_spec = {"publish": {"operationId": None, "message": None}}

            self.spec["channels"][topic] = event_spec

        return self.spec

    def get_events(self) -> list[dict]:
        config = MinosConfig(self.CONFIG_FILE_PATH)
        events = list()
        for name in config.services:
            decorators = EnrouteAnalyzer(name, config).get_broker_event()
            events += [{"topic": decorator.topic} for decorator in set(chain(*decorators.values()))]

        return events


BASE_SPEC = {
    "asyncapi": "2.0.0",
    "info": {"title": None, "version": None},
    "description": None,
    "license": "MIT",
    "channels": {},
}
