import logging
from collections.abc import (
    Hashable,
)

import aiopg
from aiomisc.pool import (
    ContextManager,
)
from aiopg import (
    Connection,
)
from psycopg2 import (
    OperationalError,
)

from ..pools import (
    MinosPool,
)
from .locks import (
    PostgreSqlLock,
)

logger = logging.getLogger(__name__)


class PostgreSqlPool(MinosPool[ContextManager]):
    """Postgres Pool class."""

    def __init__(self, host: str, port: int, database: str, user: str, password: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    @classmethod
    def _from_config(cls, *args, config, **kwargs):
        return cls(*args, **config.repository._asdict(), **kwargs)

    async def _create_instance(self) -> Connection:
        connection = await aiopg.connect(
            host=self.host, port=self.port, dbname=self.database, user=self.user, password=self.password
        )
        logger.info(f"Created {self.database!r} database connection identified by {id(connection)}!")
        return connection

    async def _destroy_instance(self, instance: Connection):
        if not instance.closed:
            await instance.close()
        logger.info(f"Destroyed {self.database!r} database connection identified by {id(instance)}!")

    async def _check_instance(self, instance: Connection) -> bool:
        try:
            # This operation connects to the database and raises an exception if something goes wrong.
            instance.isolation_level
        except OperationalError:
            return False

        return not instance.closed


class PostgreSqlLockPool(PostgreSqlPool):
    """Postgres Locking Pool class."""

    def acquire(self, key: Hashable, *args, **kwargs) -> PostgreSqlLock:
        """Acquire a new lock.

        :param key: The key to be used for locking.
        :return: A ``PostgreSqlLock`` instance.
        """
        return PostgreSqlLock(super().acquire(), key, *args, **kwargs)
