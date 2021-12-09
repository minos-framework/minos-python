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


class RestEnrouteHandleDecorator(EnrouteHandleDecorator, ABC):
    """Rest Enroute class"""

    def __init__(self, url: str, method: str):
        self.url = url
        self.method = method

    def __iter__(self) -> Iterable:
        yield from (
            self.url,
            self.method,
        )


class RestCommandEnrouteDecorator(RestEnrouteHandleDecorator):
    """Rest Command Enroute class"""

    KIND: Final[EnrouteHandleDecoratorKind] = EnrouteHandleDecoratorKind.Command


class RestQueryEnrouteDecorator(RestEnrouteHandleDecorator):
    """Rest Query Enroute class"""

    KIND: Final[EnrouteHandleDecoratorKind] = EnrouteHandleDecoratorKind.Query
