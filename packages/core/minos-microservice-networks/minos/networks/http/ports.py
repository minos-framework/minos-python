from __future__ import (
    annotations,
)

import logging
from typing import (
    Optional,
)

from cached_property import (
    cached_property,
)
from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    NotProvidedException,
)

from ..ports import (
    Port,
)
from .connectors import (
    HttpConnector,
)

logger = logging.getLogger(__name__)


class HttpPort(Port):
    """Http Port class."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_kwargs = kwargs

    async def start(self) -> None:
        """Start the service execution.

        :return: This method does not return anything.
        """
        await self.application.setup()

    async def stop(self, err: Exception = None) -> None:
        """Stop the service execution.

        :param err: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await self.application.destroy()

    @cached_property
    def application(self) -> HttpConnector:
        """Get the service handler.

        :return: A ``Handler`` instance.
        """
        return self._get_application(**self._init_kwargs)

    @staticmethod
    @inject
    def _get_application(
        application: Optional[HttpConnector] = None,
        http_application: Optional[HttpConnector] = Provide["http_application"],
        **kwargs,
    ) -> HttpConnector:
        if application is None:
            application = http_application
        if application is None or isinstance(application, Provide):
            raise NotProvidedException(f"A {HttpConnector!r} must be provided.")
        return application
