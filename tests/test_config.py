import pytest

from minos.common.configuration.config import MinosConfig
from minos.common.exceptions import MinosConfigException


def test_config_ini_fail():
    with pytest.raises(MinosConfigException):
        instance = MinosConfig(path='./test_fail_config.yaml')


def test_config_service():
    provider_instance = MinosConfig(path='./tests/test_config.yaml')
    assert provider_instance.service.name == "Order"


def test_config_rest():
    provider_instance = MinosConfig(path='./tests/test_config.yaml')
    assert provider_instance.rest.broker.host == "localhost"
    assert provider_instance.rest.broker.port == 8900
    assert provider_instance.rest.endpoints[0].name == "AddOrder"


def test_config_events():
    provider_instance = MinosConfig(path='./tests/test_config.yaml')
    assert provider_instance.events.broker.host == "localhost"
    assert provider_instance.events.broker.port == 9092


def test_config_events_database():
    provider_instance = MinosConfig(path='./tests/test_config.yaml')
    assert provider_instance.events.database.path == "./tests/local_db.lmdb"
    assert provider_instance.events.database.name == "database_events_test"


def test_config_commands_database():
    provider_instance = MinosConfig(path='./tests/test_config.yaml')
    assert provider_instance.commands.database.path == "./tests/local_db.lmdb"
    assert provider_instance.commands.database.name == "database_commands_test"
