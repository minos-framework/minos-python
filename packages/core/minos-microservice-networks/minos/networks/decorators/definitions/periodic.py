from abc import (
    ABC,
)
from collections.abc import (
    Iterable,
)
from typing import (
    Final,
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
        yield from (self.crontab.matchers,)


class PeriodicEventEnrouteDecorator(PeriodicEnrouteDecorator):
    """Periodic Event Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Event
