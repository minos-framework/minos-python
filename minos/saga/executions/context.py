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

    content: dict[str, MinosModel]

    def __init__(self, content: Optional[dict[str, (str, MinosModel)]] = None, *args, **kwargs):
        if content is None:
            content = dict()
        super().__init__(*args, content=content, **kwargs)

    def update(self, key: str, value: MinosModel) -> NoReturn:
        """TODO

        :param key: TODO
        :param value: TODO
        :return: TODO
        """
        self.content[key] = value
