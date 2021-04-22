import pytest

from minos.common.configuration.config import MinosConfig
from minos.networks.broker import MinosBrokerDatabase, BrokerDatabaseInitializer, EventBrokerQueueDispatcher


@pytest.fixture
def broker_config():
    return MinosConfig(path="./tests/test_config.yaml")


@pytest.fixture
async def database(broker_config):
    return await MinosBrokerDatabase().get_connection(broker_config)


@pytest.fixture
def services(broker_config):
    return [BrokerDatabaseInitializer(config=broker_config), EventBrokerQueueDispatcher(interval=0.5, delay=0, config=broker_config)]
