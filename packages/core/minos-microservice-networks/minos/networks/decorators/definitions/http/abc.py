import warnings
from typing import (
    Iterable,
    Optional,
)

from ..abc import (
    EnrouteDecorator,
)


class HttpEnrouteDecorator(EnrouteDecorator):
    """Http Enroute Decorator class."""

    def __init__(self, path: Optional[str] = None, method: Optional[str] = None, url: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        if path is None and url is not None:
            warnings.warn("The 'url' argument has been deprecated. 'path' must be used.", DeprecationWarning)
            path = url
        if path is None:
            raise ValueError("A 'path' must be provided.")
        if method is None:
            raise ValueError("A 'method' must be provided.")
        self.path = path
        self.method = method

    def __iter__(self) -> Iterable:
        yield from (
            self.path,
            self.method,
        )

    @property
    def url(self) -> str:
        """Get the url.

        :return: A ``str`` value.
        """
        return self.path
