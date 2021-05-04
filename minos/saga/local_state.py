"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from minos.common import MinosStorage


class MinosLocalState:
    def __init__(self, storage: MinosStorage, db_name: str):
        self._storage = storage
        self.db_name = db_name

    def update(self, key: str, value: str):
        actual_state = self._storage.get(self.db_name, key)
        if actual_state is not None:
            self._storage.update(self.db_name, key, value)
        else:
            self._storage.add(self.db_name, key, value)

    def add(self, key: str, value: str):
        self._storage.add(self.db_name, key, value)

    def load_state(self, key: str):
        actual_state = self._storage.get(self.db_name, key)
        return actual_state

    def delete_state(self, key: str):
        self._storage.delete(self.db_name, key)
