"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from pathlib import (
    Path,
)

from minos.common import (
    Aggregate,
)

BASE_PATH = Path(__file__).parent


class NaiveAggregate(Aggregate):
    """Naive aggregate class to be used for testing purposes."""
    test: int
