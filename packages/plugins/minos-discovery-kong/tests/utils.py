from pathlib import (
    Path,
)
from typing import (
    Any,
    Optional,
    Union,
)

from minos.networks import (
    HttpRequest,
    InMemoryRequest,
)

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"
rand = 27
TEST_HOST = f"{rand}.{rand}.{rand}.{rand}"


class InMemoryHttpRequest(InMemoryRequest, HttpRequest):
    """For testing purposes."""

    def __init__(
        self,
        *args,
        headers: Optional[dict[str, str]] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if headers is None:
            headers = dict()
        self._headers = headers

    @property
    def headers(self) -> dict[str, str]:
        """For testing purposes."""
        return self._headers

    @property
    def content_type(self) -> str:
        """For testing purposes."""
        return ""

    async def url_params(self, type_: Optional[Union[type, str]] = None, **kwargs) -> Any:
        """For testing purposes."""

    @property
    def has_url_params(self) -> bool:
        """For testing purposes."""
        return False

    async def query_params(self, type_: Optional[Union[type, str]] = None, **kwargs) -> Any:
        """For testing purposes."""

    @property
    def has_query_params(self) -> bool:
        """For testing purposes."""
        return False
