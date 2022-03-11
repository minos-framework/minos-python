__author__ = "Minos Framework Devs"
__email__ = "hey@minos.run"
__version__ = "0.6.0"

from .decorators import (
    GraphQlEnroute,
)


def _register_enroute():
    GraphQlEnroute.register()
