"""minos.common.datetime module."""

from datetime import (
    datetime,
    timezone,
)


def current_datetime() -> datetime:
    """TODO

    :return: TODO
    """
    return datetime.now(tz=timezone.utc)
