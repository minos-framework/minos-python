from __future__ import (
    annotations,
)

from typing import (
    TYPE_CHECKING,
    Any,
)

from .abc import (
    Config,
)

if TYPE_CHECKING:
    from ..injections import (
        InjectableMixin,
    )


class ConfigV2(Config):
    """TODO"""

    @property
    def _version(self) -> int:
        return 2

    def _get_name(self) -> str:
        pass

    def _get_injections(self) -> list[type[InjectableMixin]]:
        pass

    def _get_databases(self) -> dict[str, dict[str, Any]]:
        pass

    def _get_interfaces(self) -> dict[str, dict[str, Any]]:
        pass

    def _get_routers(self) -> list[type]:
        pass

    def _get_middleware(self) -> list[type]:
        pass

    def _get_services(self) -> list[type]:
        pass

    def _get_discovery(self) -> dict[str, Any]:
        pass

    def _get_aggregate(self) -> dict[str, Any]:
        pass

    def _get_saga(self) -> dict[str, Any]:
        pass
