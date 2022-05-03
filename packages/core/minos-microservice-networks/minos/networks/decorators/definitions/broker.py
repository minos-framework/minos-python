from abc import (
    ABC,
)
from typing import (
    Final,
    Iterable,
)

from .abc import (
    EnrouteDecorator,
)
from .kinds import (
    EnrouteDecoratorKind,
)


class BrokerEnrouteDecorator(EnrouteDecorator, ABC):
    """Broker Enroute class"""

    def __init__(self, topic: str, **kwargs):
        super().__init__(**kwargs)
        self.topic = topic

    def __iter__(self) -> Iterable:
        yield from (self.topic,)


class BrokerCommandEnrouteDecorator(BrokerEnrouteDecorator):
    """Broker Command Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Command


class BrokerQueryEnrouteDecorator(BrokerEnrouteDecorator):
    """Broker Query Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Query


class BrokerEventEnrouteDecorator(BrokerEnrouteDecorator):
    """Broker Event Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Event
