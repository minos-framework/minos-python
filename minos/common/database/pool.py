"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from typing import (
    AsyncContextManager,
)

import aiopg
from aiomisc.pool import (
    ContextManager,
)
from aiopg import (
    Connection,
    Cursor,
)

from ..pools import (
    MinosPool,
)


class PostgreSqlPool(MinosPool):
    """Postgres Pool class."""

    def __init__(self, host: str, port: int, database: str, user: str, password: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    async def _create_instance(self) -> Connection:
        connection = await aiopg.connect(
            host=self.host, port=self.port, dbname=self.database, user=self.user, password=self.password
        )
        return connection

    async def _destroy_instance(self, instance: Connection):
        if not instance.closed:
            await instance.close()

    def cursor(self, *args, **kwargs) -> AsyncContextManager[Cursor]:
        """Get a new cursor.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A Cursor wrapped into an asynchronous context manager.
        """
        acquired: ContextManager = self.acquire()

        async def _fn_enter():
            connection = await acquired.__aenter__()
            cursor = await connection.cursor(*args, **kwargs).__aenter__()
            return cursor

        async def _fn_exit(cursor: Cursor):
            if not cursor.closed:
                cursor.close()
            await acquired.__aexit__(None, None, None)

        return ContextManager(_fn_enter, _fn_exit)
