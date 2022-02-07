from itertools import chain
from operator import itemgetter
from pathlib import Path

from minos.common import MinosConfig
from ..networks import enroute, EnrouteAnalyzer


class OpenAPIService:
    CONFIG_FILE_PATH = Path(__file__).parent / "config.yml"

    def __init__(self):
        self.spec = BASE_SPEC.copy()

    @enroute.rest.command("/spec/openapi")
    def generate_spec(self) -> dict:
        endpoints = self.get_endpoints()

        for endpoint in endpoints:
            url = endpoint["url"]
            method = endpoint["method"]

            endpoint_spec = {
                "description": None,
                "produces": [None],
                "parameters": [None],
                "responses": {"200": {}, "default": {}},
            }

            if url in self.spec["paths"]:
                self.spec["paths"][url][method] = endpoint_spec
            else:
                self.spec["paths"][url] = {method: endpoint_spec}

            return self.spec

    def get_endpoints(self) -> list[dict]:
        config = MinosConfig(self.CONFIG_FILE_PATH)
        endpoints = list()
        for name in config.services:
            decorators = EnrouteAnalyzer(name, config).get_rest_command_query()
            endpoints += [
                {"url": decorator.url, "method": decorator.method} for decorator in set(chain(*decorators.values()))
            ]

        endpoints.sort(key=itemgetter("url", "method"))

        return endpoints


BASE_SPEC = {
    "swagger": "2.0",
    "info": {
        "version": "1.0.0",
        "title": "Minos OpenAPI Spec",
        "description": "An example API",
        "contact": {"name": "Minos framework"},
        "license": {"name": "MIT"},
    },
    "host": "TODO",
    "basePath": "/api/specs/openapi",
    "schemes": ["http"],
    "consumes": ["application/json"],
    "produces": ["application/json"],
    "paths": {},
    "definitions": {
        "Pet": {
            "type": "object",
            "required": ["id", "name"],
            "properties": {
                "id": {"type": "integer", "format": "int64"},
                "name": {"type": "string"},
                "tag": {"type": "string"},
            },
        }
    },
}
