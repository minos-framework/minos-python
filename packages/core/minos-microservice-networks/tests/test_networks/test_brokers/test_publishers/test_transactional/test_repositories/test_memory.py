import unittest

from minos.networks import (
    BrokerPublisherTransactionRepository,
    InMemoryBrokerPublisherTransactionRepository,
)
from minos.networks.testing import (
    BrokerPublisherTransactionRepositoryTestCase,
)
from tests.utils import (
    NetworksTestCase,
)


class TestInMemoryBrokerPublisherTransactionRepository(NetworksTestCase, BrokerPublisherTransactionRepositoryTestCase):
    __test__ = True

    def build_repository(self) -> BrokerPublisherTransactionRepository:
        return InMemoryBrokerPublisherTransactionRepository.from_config(self.config)


if __name__ == "__main__":
    unittest.main()
