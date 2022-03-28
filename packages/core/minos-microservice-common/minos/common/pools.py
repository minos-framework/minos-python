import logging
import warnings
from abc import (
    ABC,
)
from asyncio import (
    sleep,
)
from typing import (
    Any,
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

from .setup import (
    SetupMixin,
)

logger = logging.getLogger(__name__)

P = TypeVar("P")


class Pool(SetupMixin, PoolBase, Generic[P], ABC):
    """Base class for Pool implementations in minos"""

    def __init__(self, *args, maxsize: int = 10, recycle: Optional[int] = 300, already_setup: bool = True, **kwargs):
        super().__init__(*args, maxsize=maxsize, recycle=recycle, already_setup=already_setup, **kwargs)

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
        await self._PoolBase__release(instance)
        logger.debug(f"Released instance: {instance!r}")

    def acquire(self, *args, **kwargs) -> P:
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

        await self.close()

    async def _check_instance(self, instance: P) -> bool:
        return True


class MinosPool(Pool, Generic[P], ABC):
    """MinosPool class."""

    def __init__(self, *args, **kwargs):
        warnings.warn(f"{MinosPool!r} has been deprecated. Use {Pool} instead.", DeprecationWarning)
        super().__init__(*args, **kwargs)
