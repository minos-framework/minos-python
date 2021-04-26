import pytest
from minos.common import (
    MinosBaseBroker,
)


class MinosBroker(MinosBaseBroker):
    def __init__(self):
        self.database = self._database()

    def _database(self):
        pass

    def send(self):
        pass


def test_minos_base_broker():
    assert MinosBroker().send() is None
