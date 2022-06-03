"""The aiohttp plugin of the Minos Framework."""

__author__ = "Minos Framework Devs"
__email__ = "hey@minos.run"
__version__ = "0.8.0.dev2"

from .connectors import (
    AioHttpConnector,
)
from .requests import (
    AioHttpRequest,
    AioHttpResponse,
    AioHttpResponseException,
)
