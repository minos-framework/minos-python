from datetime import (
    datetime,
)
from typing import (
    Any,
    Optional,
)
from uuid import (
    UUID,
)

from ..messages import (
    Request,
)


class SchedulingRequest(Request):
    """TODO"""

    def __init__(self, scheduled_at: datetime):
        super().__init__()
        self._scheduled_at = scheduled_at

    @property
    def user(self) -> Optional[UUID]:
        """TODO

        :return: TODO
        """
        return None

    async def content(self, **kwargs) -> Any:
        """TODO

        :param kwargs: TODO
        :return: TODO
        """
        return {"scheduled_at": self._scheduled_at}

    def __eq__(self, other: Request) -> bool:
        return False

    def __repr__(self) -> str:
        return ""
