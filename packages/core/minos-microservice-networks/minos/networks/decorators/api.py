from __future__ import (
    annotations,
)

from .definitions import (
    BrokerCommandEnrouteDecorator,
    BrokerEventEnrouteDecorator,
    BrokerQueryEnrouteDecorator,
    PeriodicEventEnrouteDecorator,
    RestCommandEnrouteDecorator,
    RestQueryEnrouteDecorator,
)


class BrokerEnroute:
    """Broker Enroute class"""

    command = BrokerCommandEnrouteDecorator
    query = BrokerQueryEnrouteDecorator
    event = BrokerEventEnrouteDecorator


class RestEnroute:
    """Rest Enroute class"""

    command = RestCommandEnrouteDecorator
    query = RestQueryEnrouteDecorator


class PeriodicEnroute:
    """Periodic Enroute class."""

    event = PeriodicEventEnrouteDecorator


class Enroute:
    """Enroute decorator main class"""

    broker = BrokerEnroute
    rest = RestEnroute
    periodic = PeriodicEnroute


enroute = Enroute
