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
        await self.connector.setup()

    async def stop(self, err: Exception = None) -> None:
        """Stop the service execution.

        :param err: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await self.connector.destroy()

    @cached_property
    def connector(self) -> HttpConnector:
        """Get the port connector.

        :return: A ``HttpConnector`` instance.
        """
        return self._get_connector(**self._init_kwargs)

    @staticmethod
    @inject
    def _get_connector(
        connector: Optional[HttpConnector] = None,
        http_connector: Optional[HttpConnector] = Provide["http_connector"],
        **kwargs,
    ) -> HttpConnector:
        if connector is None:
            connector = http_connector
        if connector is None or isinstance(connector, Provide):
            raise NotProvidedException(f"A {HttpConnector!r} must be provided.")
        return connector
