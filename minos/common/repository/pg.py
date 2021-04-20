"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    NoReturn,
)

from .abc import (
    MinosRepository,
)
from .entries import (
    MinosRepositoryEntry,
)


class PostgreSqlMinosRepository(MinosRepository):
    """TODO"""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def _generate_next_id(self) -> int:
        raise NotImplementedError

    def _generate_next_aggregate_id(self, aggregate_name: str) -> int:
        raise NotImplementedError

    def _get_next_version_id(self, aggregate_name: str, aggregate_id: int) -> int:
        raise NotImplementedError

    def _submit(self, entry: MinosRepositoryEntry) -> NoReturn:
        raise NotImplementedError

    def select(self, *args, **kwargs) -> list[MinosRepositoryEntry]:
        """TODO

        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        raise NotImplementedError
