"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from .definitions import (
    BrokerCommandEnrouteDecorator,
    BrokerEventEnrouteDecorator,
    BrokerQueryEnrouteDecorator,
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


class Enroute:
    """Enroute decorator main class"""

    broker = BrokerEnroute
    rest = RestEnroute


enroute = Enroute
