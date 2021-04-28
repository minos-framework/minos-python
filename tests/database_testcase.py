"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import os
import unittest

import aiopg
from minos.common import (
    MinosConfig,
)
from tests.utils import (
    BASE_PATH,
)


class PostgresAsyncTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config = MinosConfig(BASE_PATH / "test_config.yaml")
        self._meta_kwargs = {
            "host": os.getenv("MINOS_REPOSITORY_HOST", "localhost"),
            "port": os.getenv("MINOS_REPOSITORY_PORT", 5432),
            "database": os.getenv("MINOS_REPOSITORY_DATABASE", "postgres"),
            "user": os.getenv("MINOS_REPOSITORY_USER", "postgres"),
            "password": os.getenv("MINOS_REPOSITORY_PASSWORD", "postgres"),
        }

        self.kwargs = self._meta_kwargs | {
            "database": "test_db",
            "user": "test_user",
            "password": "test_password",
        }

    async def asyncSetUp(self):
        async with aiopg.connect(**self._meta_kwargs) as connection:
            async with connection.cursor() as cursor:
                template = "DROP DATABASE IF EXISTS {database};"
                await cursor.execute(template.format(**self.kwargs))

                template = "DROP ROLE IF EXISTS {user};"
                await cursor.execute(template.format(**self.kwargs))

                template = "CREATE ROLE {user} WITH SUPERUSER CREATEDB LOGIN ENCRYPTED PASSWORD {password!r};"
                await cursor.execute(template.format(**self.kwargs))

                template = "CREATE DATABASE {database} WITH OWNER = {user};"
                await cursor.execute(template.format(**self.kwargs))

    async def asyncTearDown(self):
        async with aiopg.connect(**self._meta_kwargs) as connection:
            async with connection.cursor() as cursor:
                template = "DROP DATABASE IF EXISTS {database}"
                await cursor.execute(template.format(**self.kwargs))
