import sys
import unittest
import warnings

from minos.common import (
    Config,
    DependencyInjector,
    MinosSetup,
    NotProvidedException,
    Object,
    SetupMixin,
)
from tests.utils import (
    BASE_PATH,
)


class TestSetupMixin(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(SetupMixin, Object))

    def test_already_setup_default(self):
        instance = _SetupMixin()
        self.assertEqual(False, instance.already_setup)
        self.assertEqual(True, instance.already_destroyed)
        self.assertEqual(0, instance.setup_calls)
        self.assertEqual(0, instance.destroy_calls)

    def test_already_setup_true(self):
        instance = _SetupMixin(already_setup=True)
        self.assertEqual(True, instance.already_setup)
        self.assertEqual(False, instance.already_destroyed)
        self.assertEqual(0, instance.setup_calls)
        self.assertEqual(0, instance.destroy_calls)

    async def test_setup_destroy(self):
        instance = _SetupMixin()
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
        instance = _SetupMixin(already_setup=True)
        await instance.setup()
        self.assertEqual(True, instance.already_setup)
        self.assertEqual(False, instance.already_destroyed)
        self.assertEqual(0, instance.setup_calls)
        self.assertEqual(0, instance.destroy_calls)

    async def test_destroy_already_destroyed(self):
        instance = _SetupMixin()
        await instance.destroy()
        self.assertEqual(False, instance.already_setup)
        self.assertEqual(True, instance.already_destroyed)
        self.assertEqual(0, instance.setup_calls)
        self.assertEqual(0, instance.destroy_calls)

    def test_from_config(self):
        config = Config(BASE_PATH / "test_config.yml")
        _SetupMixin.from_config(config)

    def test_from_config_file_path(self):
        _SetupMixin.from_config(BASE_PATH / "test_config.yml")

    async def test_from_config_with_dependency_injection(self):
        config = Config(BASE_PATH / "test_config.yml")
        injector = DependencyInjector(config)
        await injector.wire(modules=[sys.modules[__name__]])

        _SetupMixin.from_config()

        await injector.unwire()

    def test_from_config_raises(self):
        with self.assertRaises(NotProvidedException):
            _SetupMixin.from_config()

    def test_del(self):
        instance = _SetupMixin(already_setup=True)
        with self.assertWarns(ResourceWarning):
            del instance


class TestMinosSetup(unittest.TestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(MinosSetup, SetupMixin))

    def test_warnings(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            setup = MinosSetup()
            self.assertIsInstance(setup, SetupMixin)


class _SetupMixin(SetupMixin):
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
