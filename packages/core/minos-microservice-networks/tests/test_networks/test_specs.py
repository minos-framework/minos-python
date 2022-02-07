import unittest
from itertools import chain
from operator import itemgetter

from minos.common import (
    MinosConfig,
)
from minos.networks import EnrouteAnalyzer
from tests.utils import (
    BASE_PATH,
)


class TestAPISpecs(unittest.TestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_true(self):
        self.assertEqual(True, True)

    def test_openapi(self):
        config = MinosConfig(self.CONFIG_FILE_PATH)
        endpoints = list()
        for name in config.services:
            decorators = EnrouteAnalyzer(name, config).get_rest_command_query()
            endpoints += [
                {"url": decorator.url, "method": decorator.method} for decorator in set(chain(*decorators.values()))
            ]

        endpoints.sort(key=itemgetter("url", "method"))

        base_spec = {
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

        for endpoint in endpoints:
            url = endpoint["url"]
            method = endpoint["method"]

            openapi_path_spec = {
                "description": None,
                "produces": [None],
                "parameters": [None],
                "responses": {"200": {}, "default": {}},
            }

            if url in base_spec["paths"]:
                base_spec["paths"][url][method] = openapi_path_spec
            else:
                base_spec["paths"][url] = {method: openapi_path_spec}

        pass

    def test_asyncapi(self):
        config = MinosConfig(self.CONFIG_FILE_PATH)

        events = list()
        for name in config.services:
            decorators = EnrouteAnalyzer(name, config).get_broker_event()
            events += [{"topic": decorator.topic} for decorator in set(chain(*decorators.values()))]

        base_spec = {
            "asyncapi": "2.0.0",
            "info": {"title": None, "version": None},
            "description": None,
            "license": "MIT",
            "servers": {"url": f"{config.broker.host}:{config.broker.port}"},
            "channels": {},
        }

        for event in events:
            topic: str = event["topic"]
            event_spec = {"publish": {"operationId": None, "message": None}}

            base_spec["channels"][topic] = event_spec

        pass


if __name__ == "__main__":
    unittest.main()
