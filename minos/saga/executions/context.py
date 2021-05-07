"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    NoReturn,
    Optional,
)

from minos.common import (
    Aggregate,
    MinosModel,
)


class SagaContext(MinosModel):
    """TODO"""

    content: dict[str, (str, Aggregate)]

    def __init__(self, *args, content: Optional[dict[str, (str, Aggregate)]] = None, **kwargs):
        if content is None:
            content = dict()
        super().__init__(*args, content=content, **kwargs)

    def update(self, name: str, aggregate: Aggregate) -> NoReturn:
        """TODO

        :param name: TODO
        :param aggregate: TODO
        :return: TODO
        """
        self.content[name] = (aggregate.classname, aggregate)
