from minos.common import (
    PostgreSqlPool,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    BASE_PATH,
)


class TestPostgreSqlPool(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        pool = PostgreSqlPool.from_config(config=self.config)
        self.assertEqual(self.config.repository.database, pool.database)
        self.assertEqual(self.config.repository.user, pool.user)
        self.assertEqual(self.config.repository.password, pool.password)
        self.assertEqual(self.config.repository.host, pool.host)
        self.assertEqual(self.config.repository.port, pool.port)
