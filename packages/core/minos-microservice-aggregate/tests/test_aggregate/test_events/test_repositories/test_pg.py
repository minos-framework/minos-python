import unittest

import aiopg

from minos.aggregate import (
    EventRepository,
    PostgreSqlEventRepository,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.testcases import (
    EventRepositorySelectTestCase,
    EventRepositorySubmitTestCase,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestPostgreSqlEventRepositorySubmit(PostgresAsyncTestCase, EventRepositorySubmitTestCase):
    __test__ = True

    CONFIG_FILE_PATH = CONFIG_FILE_PATH

    def setUp(self) -> None:
        PostgresAsyncTestCase.setUp(self)
        EventRepositorySubmitTestCase.setUp(self)

    async def asyncSetUp(self):
        await PostgresAsyncTestCase.asyncSetUp(self)
        await EventRepositorySubmitTestCase.asyncSetUp(self)

    def tearDown(self):
        EventRepositorySubmitTestCase.tearDown(self)
        PostgresAsyncTestCase.tearDown(self)

    async def asyncTearDown(self):
        await EventRepositorySelectTestCase.asyncTearDown(self)
        await PostgresAsyncTestCase.asyncTearDown(self)

    def build_event_repository(self) -> EventRepository:
        """Fort testing purposes."""
        return PostgreSqlEventRepository(**self.repository_db)

    def test_constructor(self):
        repository = PostgreSqlEventRepository("host", 1234, "database", "user", "password")
        self.assertIsInstance(repository, EventRepository)
        self.assertEqual("host", repository.host)
        self.assertEqual(1234, repository.port)
        self.assertEqual("database", repository.database)
        self.assertEqual("user", repository.user)
        self.assertEqual("password", repository.password)

    def test_from_config(self):
        repository = PostgreSqlEventRepository.from_config(self.config)
        repository_config = self.config.get_database_by_name("event")
        self.assertEqual(repository_config["database"], repository.database)
        self.assertEqual(repository_config["user"], repository.user)
        self.assertEqual(repository_config["password"], repository.password)
        self.assertEqual(repository_config["host"], repository.host)
        self.assertEqual(repository_config["port"], repository.port)

    async def test_setup(self):
        async with aiopg.connect(**self.repository_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'aggregate_event');"
                )
                response = (await cursor.fetchone())[0]
        self.assertTrue(response)


class TestPostgreSqlRepositorySelect(PostgresAsyncTestCase, EventRepositorySelectTestCase):
    __test__ = True

    CONFIG_FILE_PATH = CONFIG_FILE_PATH

    def setUp(self) -> None:
        PostgresAsyncTestCase.setUp(self)
        EventRepositorySelectTestCase.setUp(self)

    async def asyncSetUp(self):
        await PostgresAsyncTestCase.asyncSetUp(self)
        await EventRepositorySelectTestCase.asyncSetUp(self)

    def tearDown(self):
        EventRepositorySelectTestCase.tearDown(self)
        PostgresAsyncTestCase.tearDown(self)

    async def asyncTearDown(self):
        await EventRepositorySelectTestCase.asyncTearDown(self)
        await PostgresAsyncTestCase.asyncTearDown(self)

    def build_event_repository(self) -> EventRepository:
        """Fort testing purposes."""
        return PostgreSqlEventRepository(**self.repository_db)


if __name__ == "__main__":
    unittest.main()
