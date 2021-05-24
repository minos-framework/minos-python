"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from abc import (
    ABC,
)
from typing import (
    AsyncIterator,
    NoReturn,
)

from minos.common.setup import (
    MinosSetup,
)

from .pool import (
    PostgresPool,
)


class PostgreSqlMinosDatabase(ABC, MinosSetup):
    """PostgreSql Minos Database base class."""

    def __init__(self, host: str, port: int, database: str, user: str, password: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self._pool = PostgresPool(self.host, self.port, self.database, self.user, self.password)

    async def submit_query_and_fetchone(self, *args, **kwargs) -> tuple:
        """Submit a SQL query and gets the first response.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        return await self.submit_query_and_iter(*args, **kwargs).__anext__()

    async def submit_query_and_iter(self, *args, **kwargs) -> AsyncIterator[tuple]:
        """Submit a SQL query and return an asynchronous iterator.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        async with self._pool.acquire() as connection:
            with await connection.cursor() as cursor:
                await cursor.execute(*args, **kwargs)
                async for row in cursor:
                    yield row

    async def submit_query(self, *args, **kwargs) -> NoReturn:
        """Submit a SQL query.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        async with self._pool.acquire() as connection:
            with await connection.cursor() as cursor:
                await cursor.execute(*args, **kwargs)

    @property
    async def pool(self) -> PostgresPool:
        """Get the connections pool.

        :return: A ``Pool`` object.
        """
        return self._pool
