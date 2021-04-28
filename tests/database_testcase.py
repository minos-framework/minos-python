"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from typing import (
    Any,
    NoReturn,
)

import aiopg
from minos.common import (
    MinosConfig,
)
from tests.utils import (
    BASE_PATH,
)


class PostgresAsyncTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._config = MinosConfig(BASE_PATH / "test_config.yaml")
        self._repository_db = self._config.repository._asdict()

        self._events_queue_db = self._config.events.queue._asdict()
        self._events_queue_db.pop("records")
        self._events_queue_db.pop("retry")

        self._commands_queue_db = self._config.commands.queue._asdict()
        self._commands_queue_db.pop("records")
        self._commands_queue_db.pop("retry")

        self.repository_db = self._repository_db | {
            "database": "test_repository_db",
            "user": "test_repository_user",
        }
        self.events_queue_db = self._events_queue_db | {
            "database": "test_events_queue_db",
            "user": "test_events_queue_user",
        }
        self.commands_queue_db = self._commands_queue_db | {
            "database": "test_commands_queue_db",
            "user": "test_commands_queue_user",
        }

        self.config = MinosConfig(
            BASE_PATH / "test_config.yaml",
            repository_database=self.repository_db["database"],
            repository_user=self.repository_db["user"],
            events_queue_database=self.repository_db["database"],
            events_queue_user=self.repository_db["user"],
            commands_queue_database=self.repository_db["database"],
            commands_queue_user=self.repository_db["user"],
        )

    async def asyncSetUp(self):
        await self._setup(self._repository_db, self.repository_db)
        await self._setup(self._events_queue_db, self.events_queue_db)
        await self._setup(self._commands_queue_db, self.commands_queue_db)

    @staticmethod
    async def _setup(meta: dict[str, Any], test: dict[str, Any]) -> NoReturn:
        async with aiopg.connect(**meta) as connection:
            async with connection.cursor() as cursor:
                template = "DROP DATABASE IF EXISTS {database};"
                await cursor.execute(template.format(**test))

                template = "DROP ROLE IF EXISTS {user};"
                await cursor.execute(template.format(**test))

                template = "CREATE ROLE {user} WITH SUPERUSER CREATEDB LOGIN ENCRYPTED PASSWORD {password!r};"
                await cursor.execute(template.format(**test))

                template = "CREATE DATABASE {database} WITH OWNER = {user};"
                await cursor.execute(template.format(**test))

    async def asyncTearDown(self):
        await self._teardown(self._repository_db, self.repository_db)
        await self._teardown(self._events_queue_db, self.commands_queue_db)
        await self._teardown(self._commands_queue_db, self.commands_queue_db)

    @staticmethod
    async def _teardown(meta: dict[str, Any], test: dict[str, Any]) -> NoReturn:
        async with aiopg.connect(**meta) as connection:
            async with connection.cursor() as cursor:
                template = "DROP DATABASE IF EXISTS {database}"
                await cursor.execute(template.format(**test))
