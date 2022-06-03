import unittest
from unittest.mock import (
    AsyncMock,
    call,
)
from uuid import (
    uuid4,
)

from minos.saga import (
    DatabaseSagaExecutionRepository,
    SagaContext,
    SagaManager,
    SagaRunner,
)
from tests.utils import (
    ADD_ORDER,
    SagaTestCase,
)


class TestSagaManager(SagaTestCase):
    def setUp(self):
        super().setUp()
        # noinspection PyTypeChecker
        self.manager: SagaManager = SagaManager.from_config(self.config)

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        await self.manager.setup()

    async def asyncTearDown(self) -> None:
        await self.manager.destroy()
        await super().asyncTearDown()

    def test_storage(self):
        self.assertIsInstance(self.manager.storage, DatabaseSagaExecutionRepository)

    def test_runner(self):
        self.assertIsInstance(self.manager.runner, SagaRunner)

    async def test_run(self):
        expected = object()
        mock = AsyncMock(return_value=expected)
        self.manager.runner.run = mock

        observed = await self.manager.run(ADD_ORDER, SagaContext(foo="bar"))
        self.assertEqual(expected, observed)

        self.assertEqual(
            [
                call(
                    definition=ADD_ORDER,
                    context=SagaContext(foo="bar"),
                    response=None,
                    user=None,
                    autocommit=True,
                    pause_on_disk=False,
                    raise_on_error=True,
                    return_execution=True,
                )
            ],
            mock.call_args_list,
        )

    async def test_get(self):
        uuid = uuid4()
        expected = object()
        mock = AsyncMock(return_value=expected)
        self.manager.storage.load = mock

        observed = await self.manager.get(uuid)
        self.assertEqual(expected, observed)

        self.assertEqual([call(uuid)], mock.call_args_list)


if __name__ == "__main__":
    unittest.main()
