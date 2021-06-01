"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from collections import (
    namedtuple,
)
from datetime import (
    datetime,
)
from typing import (
    Callable,
)

from minos.common import (
    MinosModel,
)

HandlerEntry = namedtuple(
    "HandlerEntry",
    {
        "id": int,
        "topic": str,
        "callback": Callable,
        "partition_id": int,
        "data": MinosModel,
        "retry": int,
        "created_at": datetime,
    },
)
