from __future__ import (
    annotations,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    MinosConfig,
    NotProvidedException,
)


@inject
def get_service_name(config: MinosConfig = Provide["config"]) -> str:
    """Get the service name."""
    if config is None or isinstance(config, Provide):
        raise NotProvidedException("The config object must be provided.")
    return config.service.name
