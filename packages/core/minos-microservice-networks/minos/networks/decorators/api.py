from __future__ import (
    annotations,
)

from contextlib import (
    suppress,
)
from typing import (
    Optional,
    Protocol,
)

from minos.common import (
    get_internal_modules,
)

from .definitions import (
    BrokerCommandEnrouteDecorator,
    BrokerEventEnrouteDecorator,
    BrokerQueryEnrouteDecorator,
    EnrouteDecorator,
    PeriodicEventEnrouteDecorator,
    RestCommandEnrouteDecorator,
    RestQueryEnrouteDecorator,
)


class SubEnroute(Protocol):
    """Sub Enroute protocol class."""

    command: Optional[EnrouteDecorator]
    query: Optional[EnrouteDecorator]
    event: Optional[EnrouteDecorator]


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


class EnrouteType(type):
    """Enroute type."""

    def __getattr__(cls, item: str) -> SubEnroute:
        for module in get_internal_modules():
            with suppress(AttributeError):
                # noinspection PyProtectedMember
                module._register_enroute()
        with suppress(AttributeError):
            return object.__getattribute__(cls, item)
        raise AttributeError(f"{item} not in enroute.")


class Enroute(metaclass=EnrouteType):
    """Enroute decorator main class"""

    broker = BrokerEnroute
    rest = RestEnroute
    periodic = PeriodicEnroute

    @classmethod
    def _register_sub_enroute(cls, name: str, sub_enroute) -> None:
        """Register a new sub enroute.

        :param name: The name to be registered.
        :param sub_enroute: Teh sub enroute to be registered.
        :return: This method does not return anything.
        """
        setattr(cls, name, sub_enroute)

    @classmethod
    def _unregister_sub_enroute(cls, name: str) -> None:
        """Unregister a new sub enroute.

        :param name: The name to be unregistered.
        :return: This method does not return anything.
        """
        delattr(cls, name)


enroute = Enroute
