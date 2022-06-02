import unittest

from minos.networks import (
    BrokerPublisherTransactionRepository,
    DatabaseBrokerPublisherTransactionRepository,
)
from minos.networks.testing import (
    BrokerPublisherTransactionRepositoryTestCase,
)
from tests.utils import (
    AiopgTestCase,
)


# noinspection SqlNoDataSourceInspection
class TestBrokerPublisherTransactionRepositoryTestCase(AiopgTestCase, BrokerPublisherTransactionRepositoryTestCase):
    __test__ = True

    def build_repository(self) -> BrokerPublisherTransactionRepository:
        return DatabaseBrokerPublisherTransactionRepository.from_config(self.config)


if __name__ == "__main__":
    unittest.main()
