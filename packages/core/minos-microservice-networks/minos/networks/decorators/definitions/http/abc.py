from typing import (
    Iterable,
)

from ..abc import (
    EnrouteDecorator,
)


class HttpEnrouteDecorator(EnrouteDecorator):
    """Http Enroute Decorator class."""

    def __init__(self, url: str, method: str):
        self.url = url
        self.method = method

    def __iter__(self) -> Iterable:
        yield from (
            self.url,
            self.method,
        )
