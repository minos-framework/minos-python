import logging
import warnings
from asyncio import (
    sleep,
)
from collections.abc import (
    Hashable,
)
from typing import (
    Optional,
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

from ..locks import (
    LockPool,
)
from ..pools import (
    Pool,
)
from .locks import (
    DatabaseLock,
)

logger = logging.getLogger(__name__)


class DatabaseClientPool(Pool[ContextManager]):
    """Database Client Pool class."""

    def __init__(
        self,
        database: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if host is None:
            host = "localhost"
        if port is None:
            port = 5432
        if user is None:
            user = "postgres"
        if password is None:
            password = ""

        self.database = database
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    @classmethod
    def _from_config(cls, *args, config, **kwargs):
        return cls(*args, **config.get_default_database(), **kwargs)

    async def _create_instance(self) -> Optional[Connection]:
        try:
            connection = await aiopg.connect(
                host=self.host, port=self.port, dbname=self.database, user=self.user, password=self.password
            )
        except OperationalError as exc:
            logger.warning(f"There was an {exc!r} while trying to get a database connection.")
            await sleep(1)
            return None

        logger.info(f"Created {self.database!r} database connection identified by {id(connection)}!")
        return connection

    async def _destroy_instance(self, instance: Connection):
        if not instance.closed:
            await instance.close()
        logger.info(f"Destroyed {self.database!r} database connection identified by {id(instance)}!")

    async def _check_instance(self, instance: Optional[Connection]) -> bool:
        if instance is None:
            return False

        try:
            # This operation connects to the database and raises an exception if something goes wrong.
            instance.isolation_level
        except OperationalError:
            return False

        return not instance.closed


class PostgreSqlPool(DatabaseClientPool):
    """TODO"""

    def __init__(self, *args, **kwargs):
        warnings.warn(f"{PostgreSqlPool!r} has been deprecated. Use {DatabaseClientPool} instead.", DeprecationWarning)
        super().__init__(*args, **kwargs)


class DatabaseLockPool(LockPool, DatabaseClientPool):
    """Database Lock Pool class."""

    def acquire(self, key: Hashable, *args, **kwargs) -> DatabaseLock:
        """Acquire a new lock.

        :param key: The key to be used for locking.
        :return: A ``PostgreSqlLock`` instance.
        """
        return DatabaseLock(super().acquire(), key, *args, **kwargs)


class PostgreSqlLockPool(DatabaseLockPool):
    """TODO"""

    def __init__(self, *args, **kwargs):
        warnings.warn(
            f"{PostgreSqlLockPool!r} has been deprecated. Use {PostgreSqlLockPool} instead.", DeprecationWarning
        )
        super().__init__(*args, **kwargs)
