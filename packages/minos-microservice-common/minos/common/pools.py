from abc import (
    ABC,
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
    MinosSetup,
)

P = TypeVar("P")


class MinosPool(MinosSetup, PoolBase, Generic[P], ABC):
    """Base class for Pool implementations in minos"""

    def __init__(self, *args, maxsize: int = 10, recycle: Optional[int] = 300, already_setup: bool = True, **kwargs):
        MinosSetup.__init__(self, *args, already_setup=already_setup, **kwargs)
        PoolBase.__init__(self, maxsize=maxsize, recycle=recycle)

    async def __acquire(self) -> Any:  # pragma: no cover
        # FIXME: This method inheritance should be improved.

        if self._instances.empty() and not self._semaphore.locked():
            await self._PoolBase__create_new_instance()

        instance = await self._instances.get()

        try:
            result = await self._check_instance(instance)
        except Exception:
            self._PoolBase__recycle_instance(instance)
        else:
            if not result:
                self._PoolBase__recycle_instance(instance)
                return await self._PoolBase__acquire()

        self._used.add(instance)
        return instance

    def acquire(self, *args, **kwargs) -> P:
        """Acquire a new instance wrapped on an asynchronous context manager.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: An asynchronous context manager.
        """
        return ContextManager(self.__acquire, self._PoolBase__release)

    async def _destroy(self) -> None:
        await self.close()

    async def _check_instance(self, instance: P) -> bool:
        return True
