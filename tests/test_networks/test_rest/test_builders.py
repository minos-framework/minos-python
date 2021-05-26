from minos.common import (
    MinosConfig,
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
        config = MinosConfig(self.CONFIG_FILE_PATH)
        dispatcher = RestBuilder.from_config(config=config)
        self.assertIsInstance(dispatcher, RestBuilder)

    def test_from_config_raises(self):
        with self.assertRaises(Exception):
            RestBuilder.from_config()
