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
    EnrouteAnalyzer,
    enroute,
)
from ..requests import (
    Request,
    Response,
)


class OpenAPIService:
    """TODO"""

    def __init__(self, config: Config):
        self.config = config
        self.spec = SPEC_SCHEMA.copy()

    # noinspection PyUnusedLocal
    @enroute.rest.command("/spec/openapi", "GET")
    def generate_spec(self, request: Request) -> Response:
        """TODO"""

        for endpoint in self.endpoints:
            url = endpoint["url"]
            method = endpoint["method"]

            if url in self.spec["paths"]:
                self.spec["paths"][url][method] = PATH_SCHEMA
            else:
                self.spec["paths"][url] = {method: PATH_SCHEMA}

        return Response(self.spec)

    @property
    def endpoints(self) -> list[dict]:
        """TODO"""

        endpoints = list()
        for name in self.config.services:
            decorators = EnrouteAnalyzer(name, self.config).get_rest_command_query()
            endpoints += [
                {"url": decorator.url, "method": decorator.method} for decorator in set(chain(*decorators.values()))
            ]

        endpoints.sort(key=itemgetter("url", "method"))

        return endpoints


SPEC_SCHEMA = {
    "openapi": "3.0.0",
    "info": {
        "version": "1.0.0",
        "title": "Minos OpenAPI Spec",
        "description": "Minos OpenAPI Spec",
    },
    "host": "TODO",
    "basePath": "/api/specs/openapi",
    "schemes": ["http"],
    "consumes": ["application/json"],
    "produces": ["application/json"],
    "paths": {},
}

PATH_SCHEMA = {
    "description": None,
    "produces": [],
    "parameters": [],
    "requestBody": {},
    "responses": {},
}
