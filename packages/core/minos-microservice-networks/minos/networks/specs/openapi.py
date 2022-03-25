from itertools import (
    chain,
)
from operator import (
    itemgetter,
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


class OpenAPIService:
    def __init__(self, config: Config):
        self.config = config
        self.spec = SPECIFICATION_SCHEMA.copy()

    # noinspection PyUnusedLocal
    @enroute.rest.command("/spec/openapi", "GET")
    def generate_specification(self, request: Request) -> Response:
        for endpoint in self.endpoints:
            url = endpoint["url"]
            method = endpoint["method"].lower()

            if url in self.spec["paths"]:
                self.spec["paths"][url][method] = PATH_SCHEMA
            else:
                self.spec["paths"][url] = {method: PATH_SCHEMA}

        return Response(self.spec)

    @property
    def endpoints(self) -> list[dict]:
        endpoints = list()
        for name in self.config.get_services():
            decorators = EnrouteCollector(name, self.config).get_rest_command_query()
            endpoints += [
                {"url": decorator.url, "method": decorator.method} for decorator in set(chain(*decorators.values()))
            ]

        endpoints.sort(key=itemgetter("url", "method"))

        return endpoints


SPECIFICATION_SCHEMA = {
    "openapi": "3.0.0",
    "info": {
        "version": "1.0.0",
        "title": "Minos OpenAPI Spec",
        "description": "Minos OpenAPI Spec",
    },
    "paths": {},
}

PATH_SCHEMA = {
    "responses": {"200": {"description": ""}},
}
