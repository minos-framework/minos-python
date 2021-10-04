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
    EnrouteDecorator,
)
from .kinds import (
    EnrouteDecoratorKind,
)


class PeriodicEnrouteDecorator(EnrouteDecorator, ABC):
    """Periodic Enroute class"""

    def __init__(self, crontab: Union[str, CronTab]):
        if isinstance(crontab, str):
            crontab = CronTab(crontab)
        self.crontab = crontab

    def __iter__(self) -> Iterable:
        yield from (self.crontab,)

    def __hash__(self):
        return hash(tuple((s if not isinstance(s, CronTab) else s.matchers) for s in self))


class PeriodicEventEnrouteDecorator(PeriodicEnrouteDecorator):
    """Periodic Command Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Event
