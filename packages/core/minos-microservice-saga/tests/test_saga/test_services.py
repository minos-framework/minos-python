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
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerRequest,
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
            reply = BrokerMessageV1(
                "orderReply",
                BrokerMessageV1Payload(
                    "foo", headers={"saga": str(uuid), "transactions": str(uuid), "related_services": "ticket,product"}
                ),
            )
            response = await self.service.__reply__(BrokerRequest(reply))
        self.assertEqual(None, response)
        self.assertEqual(
            [
                call(
                    response=SagaResponse("foo", {"ticket", "product"}, SagaResponseStatus.SUCCESS, uuid),
                    pause_on_disk=True,
                    raise_on_error=False,
                    return_execution=False,
                )
            ],
            run_mock.call_args_list,
        )


if __name__ == "__main__":
    unittest.main()
