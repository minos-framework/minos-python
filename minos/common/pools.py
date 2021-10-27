from abc import (
    ABC,
)
from typing import (
    Generic,
    Optional,
    TypeVar,
)

from aiomisc import (
    PoolBase,
)

from .setup import (
    MinosSetup,
)

P = TypeVar("P")


class MinosPool(MinosSetup, PoolBase, Generic[P], ABC):
    """Base class for Pool implementations in minos"""

    def __init__(self, *args, maxsize: int = 10, recycle: Optional[int] = None, already_setup: bool = True, **kwargs):
        MinosSetup.__init__(self, *args, already_setup=already_setup, **kwargs)
        PoolBase.__init__(self, maxsize=maxsize, recycle=recycle)

    def acquire(self, *args, **kwargs) -> P:
        """Acquire a new instance wrapped on an asynchronous context manager.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: An asynchronous context manager.
        """
        return super().acquire()

    async def _destroy(self) -> None:
        await self.close()

    async def _check_instance(self, instance: P) -> bool:
        return True
