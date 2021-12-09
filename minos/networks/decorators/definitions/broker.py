from abc import (
    ABC,
)
from typing import (
    Final,
    Iterable,
)

from .abc import (
    EnrouteHandleDecorator,
)
from .kinds import (
    EnrouteHandleDecoratorKind,
)


class BrokerEnrouteHandleDecorator(EnrouteHandleDecorator, ABC):
    """Broker Enroute class"""

    def __init__(self, topic: str):
        self.topic = topic

    def __iter__(self) -> Iterable:
        yield from (self.topic,)


class BrokerCommandEnrouteDecorator(BrokerEnrouteHandleDecorator):
    """Broker Command Enroute class"""

    KIND: Final[EnrouteHandleDecoratorKind] = EnrouteHandleDecoratorKind.Command


class BrokerQueryEnrouteDecorator(BrokerEnrouteHandleDecorator):
    """Broker Query Enroute class"""

    KIND: Final[EnrouteHandleDecoratorKind] = EnrouteHandleDecoratorKind.Query


class BrokerEventEnrouteDecorator(BrokerEnrouteHandleDecorator):
    """Broker Event Enroute class"""

    KIND: Final[EnrouteHandleDecoratorKind] = EnrouteHandleDecoratorKind.Event
