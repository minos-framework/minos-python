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

from ..config import (
    Config,
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
    DatabaseClientBuilder,
    UnableToConnectException,
)
from .locks import (
    DatabaseLock,
)

logger = logging.getLogger(__name__)


class DatabaseClientPool(Pool[DatabaseClient]):
    """Database Client Pool class."""

    def __init__(self, client_builder: DatabaseClientBuilder, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._client_builder = client_builder

    @classmethod
    def _from_config(cls, config: Config, identifier: Optional[str] = None, **kwargs):
        client_cls = config.get_database_by_name(identifier).get("client", AiopgDatabaseClient)
        # noinspection PyTypeChecker
        base_builder: DatabaseClientBuilder = client_cls.get_builder()
        client_builder = base_builder.with_name(identifier).with_config(config)

        return cls(client_builder=client_builder, **kwargs)

    async def _create_instance(self) -> Optional[DatabaseClient]:
        instance = self._client_builder.build()

        try:
            await instance.setup()
        except UnableToConnectException:
            await sleep(1)
            return None

        logger.info(f"Created {instance!r}!")
        return instance

    async def _destroy_instance(self, instance: DatabaseClient):
        if instance is None:
            return
        logger.info(f"Destroyed {instance!r}!")
        await instance.destroy()

    async def _check_instance(self, instance: Optional[DatabaseClient]) -> bool:
        if instance is None:
            return False
        return await instance.is_valid()

    async def _release_instance(self, instance: DatabaseClient) -> None:
        await instance.reset()

    @property
    def client_builder(self) -> DatabaseClientBuilder:
        """Get the client builder class.

        :return: A ``DatabaseClientBuilder`` instance.
        """
        return self._client_builder


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
