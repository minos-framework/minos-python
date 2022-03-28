from __future__ import (
    annotations,
)

import logging
import warnings
from typing import (
    Optional,
)

from cached_property import (
    cached_property,
)

from minos.common import (
    Inject,
    NotProvidedException,
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

    async def _start(self) -> None:
        await self.connector.setup()
        await self.connector.start()

    async def _stop(self, err: Exception = None) -> None:
        await self.connector.stop()
        await self.connector.destroy()

    @cached_property
    def connector(self) -> HttpConnector:
        """Get the port connector.

        :return: A ``HttpConnector`` instance.
        """
        return self._get_connector(**self._init_kwargs)

    @staticmethod
    @Inject()
    def _get_connector(
        connector: Optional[HttpConnector] = None,
        http_connector: Optional[HttpConnector] = None,
        **kwargs,
    ) -> HttpConnector:
        if connector is None:
            connector = http_connector
        if connector is None:
            raise NotProvidedException(f"A {HttpConnector!r} must be provided.")
        return connector


class RestService(HttpPort):
    """Rest Service class."""

    def __init__(self, **kwargs):
        warnings.warn(f"{RestService!r} has been deprecated. Use {HttpPort} instead.", DeprecationWarning)
        super().__init__(**kwargs)
