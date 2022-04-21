"""The graphql plugin of the Minos Framework."""

__author__ = "Minos Framework Devs"
__email__ = "hey@minos.run"
__version__ = "0.7.0.dev2"

from .builders import (
    GraphQLSchemaBuilder,
)
from .decorators import (
    GraphQlCommandEnrouteDecorator,
    GraphQlEnroute,
    GraphQlEnrouteDecorator,
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
