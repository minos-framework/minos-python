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

from aiomisc.pool import (
    ContextManager,
)

from ..injections import (
    Injectable,
)
from ..locks import (
    LockPool,
)
from ..pools import (
    Pool,
)
from .clients import (
    AiopgDatabaseClient,
    DatabaseClient,
    UnableToConnectException,
)
from .locks import (
    DatabaseLock,
)

logger = logging.getLogger(__name__)


class DatabaseClientPool(Pool[DatabaseClient]):
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

    async def _create_instance(self) -> Optional[DatabaseClient]:
        instance = AiopgDatabaseClient(
            host=self.host, port=self.port, database=self.database, user=self.user, password=self.password
        )

        try:
            await instance.setup()
        except UnableToConnectException:
            await sleep(1)
            return None

        logger.info(f"Created {instance!r}!")
        return instance

    async def _destroy_instance(self, instance: DatabaseClient):
        logger.info(f"Destroyed {instance!r}!")
        await instance.destroy()

    async def _check_instance(self, instance: Optional[DatabaseClient]) -> bool:
        if instance is None:
            return False
        return await instance.is_valid()


@Injectable("postgresql_pool")
class PostgreSqlPool(DatabaseClientPool):
    """PostgreSql Pool class."""

    def __init__(self, *args, **kwargs):
        warnings.warn(f"{PostgreSqlPool!r} has been deprecated. Use {DatabaseClientPool} instead.", DeprecationWarning)
        super().__init__(*args, **kwargs)


class DatabaseLockPool(DatabaseClientPool, LockPool):
    """Database Lock Pool class."""

    def acquire(self, key: Hashable, *args, **kwargs) -> DatabaseLock:
        """Acquire a new lock.

        :param key: The key to be used for locking.
        :return: A ``PostgreSqlLock`` instance.
        """
        acquired = super().acquire()

        async def _fn_enter():
            client = await acquired.__aenter__()
            return await DatabaseLock(client, key, *args, **kwargs).__aenter__()

        async def _fn_exit(lock: DatabaseLock):
            await lock.__aexit__(None, None, None)
            await acquired.__aexit__(None, None, None)

        # noinspection PyTypeChecker
        return ContextManager(_fn_enter, _fn_exit)


class PostgreSqlLockPool(DatabaseLockPool):
    """PostgreSql Lock Pool class"""

    def __init__(self, *args, **kwargs):
        warnings.warn(
            f"{PostgreSqlLockPool!r} has been deprecated. Use {PostgreSqlLockPool} instead.", DeprecationWarning
        )
        super().__init__(*args, **kwargs)
