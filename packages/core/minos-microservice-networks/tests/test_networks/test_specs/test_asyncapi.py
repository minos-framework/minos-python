import unittest

from minos.common import (
    Config,
)
from minos.networks import (
    AsyncAPIService,
    EnrouteAnalyzer,
    InMemoryRequest,
    RestCommandEnrouteDecorator,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestAsyncAPIService(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.config = Config(CONFIG_FILE_PATH)

    def test_constructor(self):
        service = AsyncAPIService(self.config)
        self.assertIsInstance(service, AsyncAPIService)
        self.assertEqual(self.config, service.config)

    def test_get_enroute(self):
        service = AsyncAPIService(self.config)
        expected = {
            service.generate_specification.__name__: {RestCommandEnrouteDecorator("/spec/asyncapi", "GET")},
        }
        observed = EnrouteAnalyzer(service, self.config).get_all()
        self.assertEqual(expected, observed)

    async def test_generate_spec(self):
        service = AsyncAPIService(self.config)

        request = InMemoryRequest()
        response = service.generate_specification(request)

        expected = {
            "TicketAdded": {"publish": {"operationId": None, "message": None}},
            "TicketDeleted": {"publish": {"operationId": None, "message": None}},
        }

        self.assertEqual(expected, (await response.content())["channels"])


if __name__ == "__main__":
    unittest.main()
