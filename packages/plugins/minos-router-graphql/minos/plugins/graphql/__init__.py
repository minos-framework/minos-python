__author__ = "Minos Framework Devs"
__email__ = "hey@minos.run"
__version__ = "0.6.0"

from .decorators import (
    GraphQlEnroute,
)
from .routers import (
    GraphQlHttpRouter,
)
from .schema_builder import (
    GraphQLSchemaBuilder,
)


def _register_enroute():
    GraphQlEnroute.register()
