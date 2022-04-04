import unittest

import aiopg

from minos.aggregate import (
    EventRepository,
    PostgreSqlEventRepository,
)
from minos.common import (
    DatabaseClientPool,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.testcases import (
    EventRepositorySelectTestCase,
    EventRepositorySubmitTestCase,
)


class TestPostgreSqlEventRepositorySubmit(EventRepositorySubmitTestCase, PostgresAsyncTestCase):
    __test__ = True

    @staticmethod
    def build_event_repository() -> EventRepository:
        """Fort testing purposes."""
        return PostgreSqlEventRepository()

    def test_constructor(self):
        pool = DatabaseClientPool.from_config(self.config)
        repository = PostgreSqlEventRepository(pool)
        self.assertIsInstance(repository, PostgreSqlEventRepository)
        self.assertEqual(pool, repository.pool)

    def test_from_config(self):
        repository = PostgreSqlEventRepository.from_config(self.config)
        repository_config = self.config.get_database_by_name("event")
        self.assertEqual(repository_config["database"], repository.database)
        self.assertEqual(repository_config["user"], repository.user)
        self.assertEqual(repository_config["password"], repository.password)
        self.assertEqual(repository_config["host"], repository.host)
        self.assertEqual(repository_config["port"], repository.port)

    async def test_setup(self):
        async with aiopg.connect(**self.config.get_default_database()) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'aggregate_event');"
                )
                response = (await cursor.fetchone())[0]
        self.assertTrue(response)


class TestPostgreSqlRepositorySelect(EventRepositorySelectTestCase, PostgresAsyncTestCase):
    __test__ = True

    @staticmethod
    def build_event_repository() -> EventRepository:
        """Fort testing purposes."""
        return PostgreSqlEventRepository()


if __name__ == "__main__":
    unittest.main()
