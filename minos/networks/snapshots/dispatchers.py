"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    NoReturn,
    Optional,
)

from minos.common import (
    MinosConfig,
)


class MinosSnapshotDispatcher(object):
    """TODO"""

    def __init__(self, *args, **kwargs):
        ...

    @classmethod
    def from_config(cls, *args, config: MinosConfig = None, **kwargs) -> Optional[MinosSnapshotDispatcher]:
        """Build a new Snapshot Dispatcher from config.
        :param args: Additional positional arguments.
        :param config: Config instance. If `None` is provided, default config is chosen.
        :param kwargs: Additional named arguments.
        :return: A `MinosRepository` instance.
        """
        if config is None:
            config = MinosConfig.get_default()
        if config is None:
            return None
        # noinspection PyProtectedMember
        return cls(*args, **kwargs)

    def dispatch(self) -> NoReturn:
        """TODO"""
