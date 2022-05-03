"""The aiohttp plugin of the Minos Framework."""

__author__ = "Minos Framework Devs"
__email__ = "hey@minos.run"
__version__ = "0.7.0.dev4"

from .connectors import (
    AioHttpConnector,
)
from .requests import (
    AioHttpRequest,
    AioHttpResponse,
    AioHttpResponseException,
)
