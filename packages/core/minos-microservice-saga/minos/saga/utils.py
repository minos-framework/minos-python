from __future__ import (
    annotations,
)

from minos.common import (
    Config,
    Inject,
    NotProvidedException,
)


@Inject()
def get_service_name(config: Config) -> str:
    """Get the service name."""
    if config is None:
        raise NotProvidedException("The config object must be provided.")
    return config.get_name()
