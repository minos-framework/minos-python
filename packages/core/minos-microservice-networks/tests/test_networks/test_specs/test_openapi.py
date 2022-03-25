import unittest

from minos.common import (
    Config,
)
from minos.networks import (
    EnrouteCollector,
    InMemoryRequest,
    OpenAPIService,
    RestCommandEnrouteDecorator,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestOpenAPIService(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.config = Config(CONFIG_FILE_PATH)

    def test_constructor(self):
        service = OpenAPIService(self.config)
        self.assertIsInstance(service, OpenAPIService)
        self.assertEqual(self.config, service.config)

    def test_get_enroute(self):
        service = OpenAPIService(self.config)
        expected = {
            service.generate_specification.__name__: {RestCommandEnrouteDecorator("/spec/openapi", "GET")},
        }
        observed = EnrouteCollector(service, self.config).get_all()
        self.assertEqual(expected, observed)

    async def test_generate_spec(self):
        service = OpenAPIService(self.config)

        request = InMemoryRequest()
        response = service.generate_specification(request)

        expected_paths = {
            "/order": {
                "DELETE": {"description": None, "produces": [], "parameters": [], "requestBody": {}, "responses": {}},
                "GET": {"description": None, "produces": [], "parameters": [], "requestBody": {}, "responses": {}},
            },
            "/ticket": {
                "POST": {"description": None, "produces": [], "parameters": [], "requestBody": {}, "responses": {}}
            },
        }

        self.assertEqual(expected_paths, (await response.content())["paths"])


if __name__ == "__main__":
    unittest.main()
