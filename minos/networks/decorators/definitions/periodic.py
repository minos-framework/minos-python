from abc import (
    ABC,
)
from typing import (
    Final,
    Iterable,
    Union,
)

from crontab import (
    CronTab,
)

from .abc import (
    EnrouteHandleDecorator,
)
from .kinds import (
    EnrouteHandleDecoratorKind,
)


class PeriodicEnrouteHandleDecorator(EnrouteHandleDecorator, ABC):
    """Periodic Enroute class"""

    def __init__(self, crontab: Union[str, CronTab]):
        if isinstance(crontab, str):
            crontab = CronTab(crontab)
        self.crontab = crontab

    def __iter__(self) -> Iterable:
        yield from (self.crontab,)

    def __hash__(self):
        return hash(tuple((s if not isinstance(s, CronTab) else s.matchers) for s in self))


class PeriodicEventEnrouteDecorator(PeriodicEnrouteHandleDecorator):
    """Periodic Event Enroute class"""

    KIND: Final[EnrouteHandleDecoratorKind] = EnrouteHandleDecoratorKind.Event
