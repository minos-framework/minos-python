import sys
import unittest
from unittest.mock import (
    MagicMock,
    call,
)

from dependency_injector.containers import (
    Container,
)

from minos.common import (
    Config,
    DependencyInjector,
    classname,
)
from tests.utils import (
    BASE_PATH,
    FakeLockPool,
)


class TestMinosDependencyInjector(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config_file_path = BASE_PATH / "test_config.yml"
        self.config = Config(path=str(self.config_file_path))

    def test_from_str(self):
        injector = DependencyInjector(self.config, [classname(FakeLockPool)])
        self.assertIsInstance(injector.lock_pool, FakeLockPool)

    def test_lock_pool(self):
        injector = DependencyInjector(self.config, [FakeLockPool])
        self.assertIsInstance(injector.lock_pool, FakeLockPool)

    def test_raises_attribute_error(self):
        injector = DependencyInjector(self.config)
        with self.assertRaises(AttributeError):
            injector.foo

    def test_container(self):
        injector = DependencyInjector(self.config)
        self.assertIsInstance(injector.container, Container)
        self.assertEqual(self.config, injector.container.config())

    def test_container_lock_pool(self):
        injector = DependencyInjector(self.config, [FakeLockPool])
        self.assertEqual(injector.lock_pool, injector.container.lock_pool())

    async def test_wire_unwire(self):
        injector = DependencyInjector(self.config, [FakeLockPool])

        mock = MagicMock()
        injector.container.wire = mock
        await injector.wire(modules=[sys.modules[__name__]])
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(modules=[sys.modules[__name__]]), mock.call_args)

        mock = MagicMock()
        injector.container.unwire = mock
        await injector.unwire()
        self.assertEqual(1, mock.call_count)


if __name__ == "__main__":
    unittest.main()
