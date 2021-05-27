from unittest.mock import (
    MagicMock,
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


class TestRestService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = RestBuilder.from_config(config=self.config)
        self.assertIsInstance(dispatcher, RestBuilder)

    def test_from_config_raises(self):
        with self.assertRaises(Exception):
            RestBuilder.from_config()

    async def test_exec(self):
        dispatcher = RestBuilder.from_config(config=self.config)
        mock = MagicMock(side_effect=dispatcher.get_app)
        dispatcher.get_app = mock
        dispatcher.get_app()

        self.assertEqual(1, mock.call_count)
