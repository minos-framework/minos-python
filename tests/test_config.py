import pytest

from minos.common.configuration.config import MinosConfig, MinosConfigProviders
from minos.common.exceptions import MinosConfigException


def test_config_ini_fail():
    with pytest.raises(MinosConfigException):
        MinosConfig.read('./test_fail_config.ini', MinosConfigProviders.INI)


def test_config_ini_provider_default_section():
    provider_instance = MinosConfig.read('./tests/test_config.ini', MinosConfigProviders.INI)
    returned_value = provider_instance.get(key="host", section="kafka")
    assert returned_value == "kafka.minos.run"
