"""
Copyright (C) 2021 Clariteia SL
This file is part of minos framework.
Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import os
import unittest

import aiopg
from minos.common.configuration.config import MinosConfig
from minos.networks.event import event_handler_table_creation


class EventHandlerPostgresAsyncTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._meta_kwargs = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": os.getenv("POSTGRES_PORT", 5432),
            "database": os.getenv("POSTGRES_DATABASE", "postgres"),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", ""),
        }

        self.kwargs = self._meta_kwargs | {
            "database": "broker_db",
            "user": "broker",
            "password": "br0k3r",
        }

    async def asyncSetUp(self):
        await event_handler_table_creation(self._broker_config())
        """
        async with aiopg.connect(**self._meta_kwargs) as connection:
            async with connection.cursor() as cursor:

                template = "DROP DATABASE IF EXISTS {database};"
                await cursor.execute(template.format(**self.kwargs))

                template = "DROP ROLE IF EXISTS {user};"
                await cursor.execute(template.format(**self.kwargs))

                template = "CREATE ROLE {user} WITH CREATEDB LOGIN ENCRYPTED PASSWORD {password!r};"
                await cursor.execute(template.format(**self.kwargs))

                template = "CREATE DATABASE {database} WITH OWNER = {user};"
                await cursor.execute(template.format(**self.kwargs))
        """

    @staticmethod
    def _broker_config():
        return MinosConfig(path="./tests/test_config.yaml")

    async def _database(self):
        conf = self._broker_config()
        db_dsn = f"dbname={conf.events.queue.database} user={conf.events.queue.user} " \
                 f"password={conf.events.queue.password} host={conf.events.queue.host}"
        return await aiopg.connect(db_dsn)

    async def asyncTearDown(self):
        pass
        """
        database = await self._database()
        async with database as connection:
            async with connection.cursor() as cursor:
                template = "DROP TABLE IF EXISTS event_queue"
                await cursor.execute(template)
        """
