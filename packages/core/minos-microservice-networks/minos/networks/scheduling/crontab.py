from __future__ import (
    annotations,
)

import asyncio
from datetime import (
    datetime,
)
from itertools import (
    count,
)
from math import (
    inf,
)
from typing import (
    Any,
    AsyncIterator,
    Optional,
    Union,
)

from crontab import CronTab as CrontTabImpl

from minos.common import (
    current_datetime,
)


class CronTab:
    """CronTab class."""

    def __init__(self, pattern: Union[str, CrontTabImpl]):
        if isinstance(pattern, str) and pattern == "@reboot":
            pattern = None
        elif not isinstance(pattern, CrontTabImpl):
            pattern = CrontTabImpl(pattern)
        self._impl = pattern

    @property
    def impl(self) -> Optional[CrontTabImpl]:
        """Get the crontab implementation.

        :return: A ``crontab.CronTab`` or ``None``.
        """
        return self._impl

    def __hash__(self):
        return hash((type(self), self._impl_matchers))

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self._impl_matchers == other._impl_matchers

    @property
    def _impl_matchers(self):
        if self._impl is None:
            return None
        return self._impl.matchers

    async def __aiter__(self) -> AsyncIterator[datetime]:
        counter = count()
        now = current_datetime()
        while next(counter) < self.repetitions:
            await self.sleep_until_next(now)
            now = current_datetime()
            yield now

        await asyncio.sleep(inf)

    async def sleep_until_next(self, *args, **kwargs) -> None:
        """Sleep until next matching.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        await asyncio.sleep(self.get_delay_until_next(*args, **kwargs))

    def get_delay_until_next(self, now: Optional[datetime] = None) -> float:
        """Get the time to wait for next matching.

        :param now: Current time.
        :return:
        """
        if self._impl is None:
            return 0

        if now is None:
            now = current_datetime()
        return self._impl.next(now)

    @property
    def repetitions(self) -> Union[int, float]:
        """Get the number of repetitions.

        :return: A ``float`` value.
        """
        if self._impl is None:
            return 1
        return inf
