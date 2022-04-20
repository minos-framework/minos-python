from __future__ import (
    annotations,
)

import logging
import warnings
from abc import (
    ABC,
)
from asyncio import (
    gather,
    sleep,
)
from typing import (
    Any,
    AsyncContextManager,
    Generic,
    Optional,
    TypeVar,
)

from aiomisc import (
    PoolBase,
)
from aiomisc.pool import (
    ContextManager,
)

from .config import (
    Config,
)
from .exceptions import (
    MinosConfigException,
    MinosException,
)
from .injections import (
    Injectable,
)
from .setup import (
    SetupMixin,
)

logger = logging.getLogger(__name__)

P = TypeVar("P")


@Injectable("pool_factory")
class PoolFactory(SetupMixin):
    """Pool Factory class."""

    _pools: dict[tuple[str, ...], Pool]

    def __init__(self, config: Config, default_classes: dict[str, type[Pool]] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if default_classes is None:
            default_classes = dict()

        self._config = config
        self._default_classes = default_classes
        self._pools = dict()

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> PoolFactory:
        return cls(config, **kwargs)

    async def _destroy(self) -> None:
        await self._destroy_pools()
        await super()._destroy()

    async def _destroy_pools(self):
        logger.debug("Destroying pools...")
        futures = (pool.destroy() for pool in self._pools.values())
        await gather(*futures)
        logger.debug("Destroyed pools!")

    def get_pool(self, type_: str, identifier: Optional[str] = None, **kwargs) -> Pool:
        """Get a pool from the factory.

        :param type_: The type of the pool.
        :param identifier:  An optional key that identifies the pool.
        :param kwargs: Additional named arguments.
        :return: A ``Pool`` instance.
        """
        key = (type_, identifier)
        if key not in self._pools:
            logger.debug(f"Creating the {key!r} pool...")
            self._pools[key] = self._create_pool(type_, identifier=identifier, **kwargs)
        return self._pools[key]

    def _create_pool(self, type_: str, **kwargs) -> Pool:
        # noinspection PyTypeChecker
        pool_cls = self._get_pool_cls(type_)
        try:
            pool = pool_cls.from_config(self._config, **kwargs)
        except MinosConfigException:
            raise PoolException("The pool could not be built.")
        return pool

    def _get_pool_cls(self, type_: str) -> type[Pool]:
        pool_cls = self._default_classes.get(type_)

        if pool_cls is None:
            pool_cls = self._config.get_pools().get("types", dict()).get(type_)

        if pool_cls is None:
            raise PoolException(
                f"There is not any provided {type!r} to build pools that matches the given type: {type_!r}"
            )

        return pool_cls


class _PoolBase(PoolBase, ABC):
    def __init__(self, *args, maxsize: int = 10, recycle: Optional[int] = None, **kwargs):
        super().__init__(maxsize=maxsize, recycle=recycle)


class Pool(SetupMixin, _PoolBase, Generic[P], ABC):
    """Base class for Pool implementations in minos"""

    def __init__(self, *args, already_setup: bool = True, **kwargs):
        super().__init__(*args, already_setup=already_setup, **kwargs)

    # noinspection PyUnresolvedReferences
    async def __acquire(self) -> Any:  # pragma: no cover
        if self._instances.empty() and not self._semaphore.locked():
            await self._PoolBase__create_new_instance()

        instance = await self._instances.get()

        # noinspection PyBroadException
        try:
            result = await self._check_instance(instance)
        except Exception:
            logger.warning("Check instance %r failed", instance)
            self._PoolBase__recycle_instance(instance)
        else:
            if not result:
                self._PoolBase__recycle_instance(instance)
                return await self._PoolBase__acquire()

        self._used.add(instance)
        logger.debug(f"Acquired instance: {instance!r}")
        return instance

    # noinspection PyUnresolvedReferences
    async def __release(self, instance: Any) -> Any:  # pragma: no cover
        await self._release_instance(instance)
        await self._PoolBase__release(instance)
        logger.debug(f"Released instance: {instance!r}")

    def acquire(self, *args, **kwargs) -> AsyncContextManager[P]:
        """Acquire a new instance wrapped on an asynchronous context manager.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: An asynchronous context manager.
        """
        # noinspection PyUnresolvedReferences
        return ContextManager(self.__acquire, self.__release)

    async def _destroy(self) -> None:
        if len(self._used):
            logger.info("Waiting for instances releasing...")
            while len(self._used):
                await sleep(0.1)
            logger.info("Released instances!")

        await self.close()

    async def _check_instance(self, instance: P) -> bool:
        return True

    async def _release_instance(self, instance: P) -> None:
        pass


class MinosPool(Pool, Generic[P], ABC):
    """MinosPool class."""

    def __init__(self, *args, **kwargs):
        warnings.warn(f"{MinosPool!r} has been deprecated. Use {Pool} instead.", DeprecationWarning)
        super().__init__(*args, **kwargs)


class PoolException(MinosException):
    """Exception to be raised when some problem related with a pool happens."""
