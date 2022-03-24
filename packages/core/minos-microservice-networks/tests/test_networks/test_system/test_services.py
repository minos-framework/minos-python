import unittest

from minos.common import (
    Config,
)
from minos.networks import (
    EnrouteCollector,
    InMemoryRequest,
    Response,
    RestCommandEnrouteDecorator,
    SystemService,
    get_host_ip,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestSystemService(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.config = Config(CONFIG_FILE_PATH)

        self.service = SystemService()

    def test_get_enroute(self):
        expected = {
            self.service.check_health.__name__: {RestCommandEnrouteDecorator("/system/health", "GET")},
        }
        observed = EnrouteCollector(self.service, self.config).get_all()
        self.assertEqual(expected, observed)

    def test_system_health(self):
        expected = Response({"host": get_host_ip()})
        observed = self.service.check_health(InMemoryRequest())
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
