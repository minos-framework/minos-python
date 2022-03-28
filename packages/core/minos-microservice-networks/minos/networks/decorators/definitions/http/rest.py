from typing import (
    Final,
)

from ..kinds import (
    EnrouteDecoratorKind,
)
from .abc import (
    HttpEnrouteDecorator,
)


class RestEnrouteDecorator(HttpEnrouteDecorator):
    """Rest Enroute class"""


class RestCommandEnrouteDecorator(RestEnrouteDecorator):
    """Rest Command Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Command


class RestQueryEnrouteDecorator(RestEnrouteDecorator):
    """Rest Query Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Query
