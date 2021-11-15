import unittest
from unittest.mock import (
    call,
    patch,
)
from uuid import (
    uuid4,
)

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    BrokerCommandEnrouteDecorator,
    CommandReply,
    CommandStatus,
    HandlerRequest,
)
from minos.saga import (
    SagaManager,
    SagaResponse,
    SagaResponseStatus,
    SagaService,
)
from tests.utils import (
    BASE_PATH,
    MinosTestCase,
)


class TestSagaService(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.config = MinosConfig(BASE_PATH / "config.yml")

        self.saga_manager = SagaManager.from_config(self.config)
        self.service = SagaService(saga_manager=self.saga_manager)

    def test_get_enroute(self):
        expected = {
            "__reply__": {BrokerCommandEnrouteDecorator("orderReply")},
        }
        observed = SagaService.__get_enroute__(self.config)
        self.assertEqual(expected, observed)

    async def test_reply(self):
        uuid = uuid4()
        with patch("minos.saga.SagaManager.run") as run_mock:
            reply = CommandReply("orderReply", "foo", uuid, CommandStatus.SUCCESS, "ticket")
            response = await self.service.__reply__(HandlerRequest(reply))
        self.assertEqual(None, response)
        self.assertEqual(
            [
                call(
                    response=SagaResponse("foo", SagaResponseStatus.SUCCESS, "ticket", uuid),
                    pause_on_disk=True,
                    raise_on_error=False,
                    return_execution=False,
                )
            ],
            run_mock.call_args_list,
        )


if __name__ == "__main__":
    unittest.main()
