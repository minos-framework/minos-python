"""The kong plugin of the Minos Framework."""

__author__ = "Minos Framework Devs"
__email__ = "hey@minos.run"
__version__ = "0.8.0.dev1"

from .client import (
    KongClient,
)
from .discovery import (
    KongDiscoveryClient,
)
from .middleware import (
    middleware,
)
