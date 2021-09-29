from __future__ import (
    annotations,
)

from datetime import (
    datetime,
)
from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    DeclarativeModel,
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

    async def content(self, **kwargs) -> SchedulingRequestContent:
        """TODO

        :param kwargs: TODO
        :return: TODO
        """
        return self._content

    def __eq__(self, other: Request) -> bool:
        return isinstance(other, type(self)) and self._content == other._content

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._content!r})"

    @property
    def _content(self) -> SchedulingRequestContent:
        return SchedulingRequestContent(self._scheduled_at)


class SchedulingRequestContent(DeclarativeModel):
    """TODO"""

    scheduled_at: datetime
