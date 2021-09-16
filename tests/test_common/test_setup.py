import unittest

from minos.common import (
    MinosConfig,
    MinosConfigNotProvidedException,
    MinosSetup,
)
from tests.utils import (
    BASE_PATH,
)


class TestMinosSetup(unittest.IsolatedAsyncioTestCase):
    def test_already_setup_default(self):
        instance = _MinosSetupMock()
        self.assertEqual(False, instance.already_setup)
        self.assertEqual(True, instance.already_destroyed)
        self.assertEqual(0, instance.setup_calls)
        self.assertEqual(0, instance.destroy_calls)

    def test_already_setup_true(self):
        instance = _MinosSetupMock(already_setup=True)
        self.assertEqual(True, instance.already_setup)
        self.assertEqual(False, instance.already_destroyed)
        self.assertEqual(0, instance.setup_calls)
        self.assertEqual(0, instance.destroy_calls)

    async def test_setup_destroy(self):
        instance = _MinosSetupMock()
        await instance.setup()
        self.assertEqual(True, instance.already_setup)
        self.assertEqual(False, instance.already_destroyed)
        self.assertEqual(1, instance.setup_calls)
        self.assertEqual(0, instance.destroy_calls)

        await instance.destroy()
        self.assertEqual(False, instance.already_setup)
        self.assertEqual(True, instance.already_destroyed)
        self.assertEqual(1, instance.setup_calls)
        self.assertEqual(1, instance.destroy_calls)

    async def test_setup_already_setup(self):
        instance = _MinosSetupMock(already_setup=True)
        await instance.setup()
        self.assertEqual(True, instance.already_setup)
        self.assertEqual(False, instance.already_destroyed)
        self.assertEqual(0, instance.setup_calls)
        self.assertEqual(0, instance.destroy_calls)

    async def test_destroy_already_destroyed(self):
        instance = _MinosSetupMock()
        await instance.destroy()
        self.assertEqual(False, instance.already_setup)
        self.assertEqual(True, instance.already_destroyed)
        self.assertEqual(0, instance.setup_calls)
        self.assertEqual(0, instance.destroy_calls)

    def test_from_config(self):
        config = MinosConfig(BASE_PATH / "test_config.yml")
        _MinosSetupMock.from_config(config)

    def test_from_config_file_path(self):
        _MinosSetupMock.from_config(BASE_PATH / "test_config.yml")

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigNotProvidedException):
            _MinosSetupMock.from_config()

    def test_del(self):
        instance = _MinosSetupMock(already_setup=True)
        with self.assertWarns(ResourceWarning):
            del instance


class _MinosSetupMock(MinosSetup):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setup_calls = 0
        self.destroy_calls = 0

    async def _setup(self) -> None:
        self.setup_calls += 1

    async def _destroy(self) -> None:
        self.destroy_calls += 1


if __name__ == "__main__":
    unittest.main()
