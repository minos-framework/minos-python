from pathlib import (
    Path,
)
from functools import (
    total_ordering,
)
from minos.common import (
    DeclarativeModel,
)
from typing import (
    Any,
)

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"


@total_ordering
class FakeModel(DeclarativeModel):
    """For testing purposes"""

    data: Any

    def __lt__(self, other: Any) -> bool:
        # noinspection PyBroadException
        return isinstance(other, type(self)) and self.data < other.data
