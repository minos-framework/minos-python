"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from typing import (
    Optional,
)
from uuid import (
    UUID,
    uuid4,
)

from .abc import (
    DeclarativeModel,
)


class Entity(DeclarativeModel):
    uuid: UUID

    def __init__(self, uuid: Optional[UUID] = None, *args, **kwargs):
        if uuid is None:
            uuid = uuid4()
        super().__init__(uuid, *args, **kwargs)
