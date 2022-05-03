from __future__ import (
    annotations,
)

from abc import (
    ABC,
)
from collections.abc import (
    Iterable,
)
from typing import (
    TYPE_CHECKING,
    Final,
    Union,
)

from .abc import (
    EnrouteDecorator,
)
from .kinds import (
    EnrouteDecoratorKind,
)

if TYPE_CHECKING:
    from crontab import CronTab as CronTabImpl

    from ...scheduling import (
        CronTab,
    )


class PeriodicEnrouteDecorator(EnrouteDecorator, ABC):
    """Periodic Enroute class"""

    def __init__(self, crontab: Union[str, CronTab, CronTabImpl], **kwargs):
        super().__init__(**kwargs)
        from ...scheduling import (
            CronTab,
        )

        if not isinstance(crontab, CronTab):
            crontab = CronTab(crontab)
        self.crontab = crontab

    def __iter__(self) -> Iterable:
        yield from (self.crontab,)


class PeriodicEventEnrouteDecorator(PeriodicEnrouteDecorator):
    """Periodic Event Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Event
