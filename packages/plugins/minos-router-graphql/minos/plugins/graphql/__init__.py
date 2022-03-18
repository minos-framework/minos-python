__author__ = "Minos Framework Devs"
__email__ = "hey@minos.run"
__version__ = "0.0.0"

from .builders import (
    GraphQLSchemaBuilder,
    GraphQlSchemaEncoder,
)
from .decorators import (
    GraphQlCommandEnrouteDecorator,
    GraphQlEnroute,
    GraphQlQueryEnrouteDecorator,
)
from .handlers import (
    GraphQlHandler,
)
from .routers import (
    GraphQlHttpRouter,
)


def _register_enroute():
    GraphQlEnroute.register()
