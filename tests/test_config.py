import pytest

from minos.common.configuration.config import MinosConfig
from minos.common.exceptions import MinosConfigException


def test_config_ini_fail():
    with pytest.raises(MinosConfigException):
        instance = MinosConfig(path='./test_fail_config.ini')


def test_config_kafka():
    provider_instance = MinosConfig(path='./tests/test_config.ini')
    kafka = provider_instance.kafka
    assert kafka.host == "kafka.minos.run"
    assert kafka.port == 9092


def test_config_rest():
    provider_instance = MinosConfig(path='./tests/test_config.ini')
    rest = provider_instance.rest
    assert rest.host == "localhost"
    assert rest.port == 8900


def test_config_microservice():
    provider_instance = MinosConfig(path='./tests/test_config.ini')
    microservice = provider_instance.microservice
    assert isinstance(microservice.events, list) == True
    assert isinstance(microservice.saga, list) == True
    assert microservice.events[0] == "OrderAdded"
    assert microservice.saga[0] == "CreateOrder"
