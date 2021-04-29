"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import Any

from aiomisc.service.periodic import PeriodicService
from minos.common import MinosConfig

from minos.networks.snapshots import MinosSnapshotDispatcher


class MinosSnapshotService(PeriodicService):
    """TODO"""

    def __init__(self, config: MinosConfig = None, **kwargs):
        super().__init__(**kwargs)
        self.dispatcher = MinosSnapshotDispatcher.from_config(config=config)

    async def callback(self) -> Any:
        """TODO

        :return: TODO
        """
        pass
