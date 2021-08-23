"""minos.common.datetime module."""

from datetime import (
    datetime,
    timezone,
)


def current_datetime() -> datetime:
    """Get current datetime in `UTC`.

    :return: A ``datetime`` instance.
    """
    return datetime.now(tz=timezone.utc)


NULL_DATETIME = datetime.max.replace(tzinfo=timezone.utc)
