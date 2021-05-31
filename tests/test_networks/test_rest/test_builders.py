import unittest

from aiohttp import (
    web,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    RestBuilder,
)
from tests.utils import (
    BASE_PATH,
)


class _Cls:
    @staticmethod
    def _fn(request, config):
        return request, config


class TestRestBuilder(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = RestBuilder.from_config(config=self.config)
        self.assertIsInstance(dispatcher, RestBuilder)

    def test_from_config_raises(self):
        with self.assertRaises(Exception):
            RestBuilder.from_config()

    def test_get_app(self):
        dispatcher = RestBuilder.from_config(config=self.config)
        self.assertIsInstance(dispatcher.get_app(), web.Application)

    def test_resolve_action(self):
        dispatcher = RestBuilder.from_config(config=self.config)

        observed = dispatcher.resolve_action(f"{__name__}._Cls", "_fn")

        observed_response = observed("request")
        self.assertEqual(("request", self.config), observed_response)


if __name__ == "__main__":
    unittest.main()
