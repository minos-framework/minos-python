"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from enum import (
    Enum,
)


class SagaStatus(Enum):
    """TODO"""

    Created = "created"
    Paused = "paused"
    Finished = "finished"
    Errored = "errored"
