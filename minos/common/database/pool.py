"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import aiomisc
import aiopg


class PostgresPool(aiomisc.PoolBase):
    def __init__(self, host: str, port: int, database: str, user: str, password: str, maxsize=10, recycle=60):
        super().__init__(maxsize=maxsize, recycle=recycle)
        self.dsn = f"dbname={database} user={user} password={password} host={host} port={port}"

    async def _create_instance(self):
        return await aiopg.create_pool(dsn=self.dsn)

    async def _destroy_instance(self, instance: aiopg.Pool):
        instance.close()
        await instance.wait_closed()

    async def _check_instance(self, instance: aiopg.Pool):
        return instance.closed()
