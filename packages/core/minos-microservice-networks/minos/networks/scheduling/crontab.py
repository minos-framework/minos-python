from __future__ import (
    annotations,
)

from typing import (
    Any,
    Union,
)

from crontab import CronTab as CrontTabImpl


class CronTab:
    """TODO"""

    def __init__(self, pattern: Union[str, CrontTabImpl]):
        if not isinstance(pattern, CrontTabImpl):
            pattern = CrontTabImpl(pattern)
        self.impl = pattern

    def __hash__(self):
        return hash(self.impl.matchers)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self.impl.matchers == other.impl.matchers

    def next(self, *args, **kwargs):
        """TODO"""
        self.impl.next(*args, **kwargs)
