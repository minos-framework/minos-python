from __future__ import (
    annotations,
)

from collections.abc import (
    Iterable,
)
from datetime import (
    timedelta,
)
from inspect import (
    iscoroutinefunction,
)
from typing import (
    TYPE_CHECKING,
    Optional,
    Union,
)

from ..callables import (
    Checker,
    CheckerMeta,
    CheckerProtocol,
)

if TYPE_CHECKING:
    from .abc import (
        Handler,
    )


class EnrouteCheckDecorator:
    """TODO"""

    def __init__(
        self,
        max_attempts: int = 10,
        delay: Union[float, timedelta] = 0.1,
        _checkers: Optional[set[CheckerMeta]] = None,
        _handler: Optional[Handler] = None,
    ):
        if isinstance(delay, timedelta):
            delay = delay.total_seconds()

        self.max_attempts = max_attempts
        self.delay = delay

        self._checkers = _checkers
        self._handler = _handler

    def __iter__(self) -> Iterable:
        yield from (
            self.delay,
            self.max_attempts,
        )

    def __call__(self, meta: Checker) -> CheckerProtocol:
        if not isinstance(meta, CheckerMeta):
            meta = getattr(meta, "meta", CheckerMeta(meta, self.max_attempts, self.delay))

        if iscoroutinefunction(meta) and not iscoroutinefunction(self._handler):
            raise Exception(f"{self._handler!r} must be a coroutine if {meta!r} is a coroutine")

        if self._checkers is not None:
            self._checkers.add(meta)

        return meta.wrapper
