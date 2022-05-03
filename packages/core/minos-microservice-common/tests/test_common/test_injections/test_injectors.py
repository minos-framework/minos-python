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
    PoolFactory,
    classname,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestDependencyInjector(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config = Config(CONFIG_FILE_PATH)

    def test_from_str(self):
        injector = DependencyInjector(self.config, [classname(PoolFactory)])
        self.assertIsInstance(injector.pool_factory, PoolFactory)

    def test_from_type(self):
        injector = DependencyInjector(self.config, [PoolFactory])
        self.assertIsInstance(injector.pool_factory, PoolFactory)

    def test_from_instance(self):
        instance = PoolFactory.from_config(self.config)
        injector = DependencyInjector(self.config, [instance])
        self.assertEqual(instance, injector.pool_factory)

    def test_raises_building(self):
        injector = DependencyInjector(self.config, ["path.to.LockPool"])
        with self.assertRaises(ValueError):
            injector.injections

    def test_raises_attribute_error(self):
        injector = DependencyInjector(self.config)
        with self.assertRaises(AttributeError):
            injector.foo

    def test_container(self):
        injector = DependencyInjector(self.config)
        self.assertIsInstance(injector.container, Container)
        self.assertEqual(self.config, injector.container.config())

    def test_container_pool_factory(self):
        injector = DependencyInjector(self.config, [PoolFactory])
        self.assertEqual(injector.pool_factory, injector.container.pool_factory())

    async def test_wire_unwire(self):
        from minos.common.injections import (
            decorators,
        )

        injector = DependencyInjector(self.config, [PoolFactory])

        mock = MagicMock()
        injector.container.wire = mock
        await injector.wire_and_setup_injections()
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(modules=[decorators]), mock.call_args)

        mock = MagicMock()
        injector.container.unwire = mock
        await injector.unwire_and_destroy_injections()
        self.assertEqual(1, mock.call_count)

    async def test_wire_unwire_with_modules(self):
        from minos.common.injections import (
            decorators,
        )

        injector = DependencyInjector(self.config, [PoolFactory])

        mock = MagicMock()
        injector.container.wire = mock
        await injector.wire_and_setup_injections(modules=[sys.modules[__name__]])
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(modules=[sys.modules[__name__], decorators]), mock.call_args)

        mock = MagicMock()
        injector.container.unwire = mock
        await injector.unwire_and_destroy_injections()
        self.assertEqual(1, mock.call_count)


if __name__ == "__main__":
    unittest.main()
