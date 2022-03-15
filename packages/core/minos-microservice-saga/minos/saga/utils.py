from __future__ import (
    annotations,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    Config,
    NotProvidedException,
)


@inject
def get_service_name(config: Config = Provide["config"]) -> str:
    """Get the service name."""
    if config is None or isinstance(config, Provide):
        raise NotProvidedException("The config object must be provided.")
    return config.service.name
