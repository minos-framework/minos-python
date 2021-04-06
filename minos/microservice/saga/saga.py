# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

import typing as t
import asyncio
import uuid
import inspect
from minos.microservice.saga.abstract import MinosBaseSagaBuilder
from minos.common.logs import log
from minos.common.storage.abstract import MinosStorage
from minos.common.storage.lmdb import MinosStorageLmdb


class MinosLocalState:
    def __init__(self, storage: MinosStorage):
        self._storage = storage
        self.db_name = "LocalState"

    def create(self, key: str, value: str):
        actual_state = self._storage.get(self.db_name, key)
        if actual_state is not None:
            self._storage.update(self.db_name, key, value)
        else:
            self._storage.add(self.db_name, key, value)

    def load_state(self, key: str):
        actual_state = self._storage.get(self.db_name, key)
        return actual_state

    def delete_state(self, key: str):
        self._storage.delete(self.db_name, key)


class MinosSagaStepManager:
    """
    Example of how states are stored:
    {
        "saga": "OrdersAdd",
        "current_step": "84a2f5be-2135-4b9e-a8ca-7ff2085c2c1e",
        "operations": {
            "bf919e2f-8a27-423f-b0a1-1fde713c1d8b": {
                "id": "bf919e2f-8a27-423f-b0a1-1fde713c1d8b",
                "type": "invokeParticipant",
                "name": "CreateOrder",
                "status": 0.0,
                "response": "_invokeParticipant Response",
                "Error": "",
            },
            "42324b59-8c6c-415b-9d6f-afab8829bd5b": {
                "id": "42324b59-8c6c-415b-9d6f-afab8829bd5b",
                "type": "invokeParticipant_callback",
                "name": "CreateOrder",
                "status": 0.0,
                "response": "create_order_callback response!!!!",
                "Error": "",
            },
            "3f164de5-4c76-454b-8028-78854eddd714": {
                "id": "3f164de5-4c76-454b-8028-78854eddd714",
                "type": "onReply",
                "name": "",
                "status": 0.0,
                "response": "async create_ticket_on_reply_callback response!!!!",
                "Error": "",
            },
            "8f28d91b-e62a-43b7-b5dc-a27e5e86bc0d": {
                "id": "8f28d91b-e62a-43b7-b5dc-a27e5e86bc0d",
                "type": "invokeParticipant",
                "name": "CreateTicket",
                "status": 0.0,
                "response": "_invokeParticipant Response",
                "Error": "",
            },
            "7b355a17-9614-4270-80c1-c7192adf1f4c": {
                "id": "7b355a17-9614-4270-80c1-c7192adf1f4c",
                "type": "onReply",
                "name": "",
                "status": 0.0,
                "response": "async create_ticket_on_reply_callback response!!!!",
                "Error": "",
            },
            "e6ae1ad7-a31b-47b8-a626-01b0f0ec28f5": {
                "id": "e6ae1ad7-a31b-47b8-a626-01b0f0ec28f5",
                "type": "invokeParticipant",
                "name": "Shipping",
                "status": 0.0,
                "response": "",
                "Error": "",
            },
            "4760bd0b-0d21-4fb4-be27-c0c85b712750": {
                "id": "4760bd0b-0d21-4fb4-be27-c0c85b712750",
                "type": "withCompensation",
                "name": "Failed",
                "status": 0.0,
                "response": "_withCompensation Response",
                "Error": "",
            },
            "c5ad3957-577d-458c-bd8a-53101acbc14b": {
                "id": "c5ad3957-577d-458c-bd8a-53101acbc14b",
                "type": "withCompensation",
                "name": "BlockOrder",
                "status": 0.0,
                "response": "_withCompensation Response",
                "Error": "",
            },
            "40d2a6a7-35ad-4f58-9e2a-97929a2ccbbf": {
                "id": "40d2a6a7-35ad-4f58-9e2a-97929a2ccbbf",
                "type": "withCompensation_callback",
                "name": "Failed_BlockOrder",
                "status": 0.0,
                "response": "_withCompensation Response",
                "Error": "",
            },
            "426a0767-6ccf-4c55-834c-4e067f38de74": {
                "id": "426a0767-6ccf-4c55-834c-4e067f38de74",
                "type": "withCompensation",
                "name": "DeleteOrder",
                "status": 0.0,
                "response": "_withCompensation Response",
                "Error": "",
            },
            "84a2f5be-2135-4b9e-a8ca-7ff2085c2c1e": {
                "id": "84a2f5be-2135-4b9e-a8ca-7ff2085c2c1e",
                "type": "withCompensation_callback",
                "name": "DeleteOrder",
                "status": 0.0,
                "response": "_withCompensation Response",
                "Error": "",
            },
        },
    }

    """
    def __init__(self, name, uuid: str, storage: MinosStorage = MinosStorageLmdb):
        self.db_name = "LocalState"
        self._storage = storage.build(path_db=self.db_name)
        self._local_state = MinosLocalState(storage=self._storage)
        self.uuid = uuid
        self.saga_name = name
        self._state = {}

    def start(self):
        structure = {
            "saga": self.saga_name,
            "current_step": None,
            "operations": {}
        }
        self._local_state.create(self.uuid, structure)

    def operation(self, step_uuid: str, type: str, name: str = ""):
        operation = {
            "id": step_uuid,
            "type": type,
            "name": name,
            "status": 0,
            "response": '',
            "Error": ''
        }

        self._state = self._local_state.load_state(self.uuid)

        self._state["current_step"] = step_uuid
        self._state["operations"][step_uuid] = operation
        self._local_state.create(self.uuid, self._state)

    def add_response(self, step_uuid: str, response: str):
        self._state = self._local_state.load_state(self.uuid)
        self._state["operations"][step_uuid]["response"] = response
        self._local_state.create(self.uuid, self._state)

    def get_last_response(self):
        self._state = self._local_state.load_state(self.uuid)
        return self._state["operations"][self._state["current_step"]]["response"]

    def close(self):
        self._state = self._local_state.load_state(self.uuid)
        log.debug(self._state)
        self._local_state.delete_state(self.uuid)


def _invokeParticipant(name):
    if name == "Shipping":
        raise Exception("invokeParticipantTest exception")
    log.debug("---> invokeParticipantTest")
    return "_invokeParticipant Response"


def _withCompensation(name):
    log.debug("---> withCompensationTest")
    return "_withCompensation Response"


class Saga(MinosBaseSagaBuilder):
    def __init__(
        self,
        name,
        step_manager: MinosSagaStepManager = MinosSagaStepManager,
        loop: asyncio.AbstractEventLoop = None,
    ):
        self.saga_name = name
        self.uuid = str(uuid.uuid4())
        self._tasks = set()
        self.saga_process = {
            "name": self.saga_name,
            "id": self.uuid,
            "steps": [],
            "current_compensations": [],
        }
        self._step_manager = step_manager(self.saga_name, self.uuid)
        self.loop = loop or asyncio.get_event_loop()
        self._response = ""

    def start(self):
        return self

    def step(self):
        self.saga_process["steps"].append([])
        return self

    async def callback_function_call_async(self, loop, func, response, callback_id):
        task = loop.create_task(func(response))  # just some task
        loop.run_until_complete(task)
        await task  # wait for it (inside a coroutine)
        response = ''
        self._step_manager.add_response(callback_id, response)

        return response

    def callback_function_call(self, func, response, callback_id):
        response = func(response)
        self._step_manager.add_response(callback_id, response)

        return response

    def _invokeParticipant(self, operation):
        self._response = _invokeParticipant(operation['name'])
        self._step_manager.add_response(operation["id"], self._response)

        if operation["callback"] is not None:
            func = operation["callback"]
            callback_id = str(uuid.uuid4())
            self._step_manager.operation(callback_id, "invokeParticipant_callback", operation["name"])
            log.debug(func)
            log.debug(asyncio.coroutines.iscoroutine(func))
            if asyncio.iscoroutine(func):
                log.debug("--------- async -----------")
                self._create_task(func, self._response, callback_id)
            else:
                log.debug("--------- sync -----------")
                self._response = self.callback_function_call(func, self._response, callback_id)
                self._step_manager.add_response(callback_id, self._response)

        return self._response

    def invokeParticipant(self, name: str, callback: t.Callable = None):
        self.saga_process["steps"][len(self.saga_process["steps"]) - 1].append(
            {
                "id": str(uuid.uuid4()),
                "type": "invokeParticipant",
                "method": self._invokeParticipant,
                "name": name,
                "callback": callback,
            }
        )

        return self

    def _withCompensation(self, operation):
        response = None
        if type(operation["name"]) == list:
            for compensation in operation["name"]:
                callback_id = str(uuid.uuid4())
                self._step_manager.operation(callback_id, operation["type"], compensation)
                response = _withCompensation(compensation)
                self._step_manager.add_response(callback_id, response)
        else:
            callback_id = str(uuid.uuid4())
            self._step_manager.operation(callback_id, operation["type"], operation["name"])
            response = _withCompensation(operation["name"])
            self._step_manager.add_response(callback_id, response)

        if operation["callback"] is not None:
            if type(operation["name"]) == list:
                name = '_'.join(operation["name"])
            else:
                name = operation["name"]

            callback_id = str(uuid.uuid4())
            self._step_manager.operation(callback_id, "withCompensation_callback", name)
            func = operation["callback"]

            if asyncio.iscoroutine(func):
                self._create_task(func(response))
            else:
                self._response = func(response)
                self._step_manager.add_response(callback_id, response)

    def withCompensation(self, name: t.Union[str, list], callback: t.Callable = None):
        self.saga_process["steps"][len(self.saga_process["steps"]) - 1].append(
            {
                "id": str(uuid.uuid4()),
                "type": "withCompensation",
                "method": self._withCompensation,
                "name": name,
                "callback": callback,
            }
        )

        return self

    def _onReply(self, operation):
        prev_response = self._step_manager.get_last_response()

        func = operation["callback"]

        callback_id = str(uuid.uuid4())

        self._step_manager.operation(callback_id, operation["type"])

        if asyncio.iscoroutine(func):
            self._create_task(func)
        else:
            self._response = func(prev_response)
            self._step_manager.add_response(callback_id, self._response)

        return self._response

    def onReply(self, _callback: t.Callable):
        self.saga_process["steps"][len(self.saga_process["steps"]) - 1].append(
            {
                "id": str(uuid.uuid4()),
                "type": "onReply",
                "method": self._onReply,
                "callback": _callback,
            }
        )

        return self

    def _execute(self):
        pass

    def execute(self):
        self.saga_process["steps"][len(self.saga_process["steps"]) - 1].append(
            {"type": "execute", "method": self._execute}
        )
        self._validate_steps()
        self._execute_steps()
        self._step_manager.close()
        return self

    def _validate_steps(self):
        for step in self.saga_process["steps"]:
            if not step:
                raise Exception("The step() cannot be empty.")

            for idx, operation in enumerate(step):
                if idx == 0 and operation["type"] is not "invokeParticipant":
                    raise Exception(
                        "The first method of the step must be .invokeParticipant(name, callback (optional))."
                    )

    def _create_task(self, coro: t.Awaitable[t.Any], response, callback_id):
        task = self.loop.create_task(self.callback_function_call_async(self.loop, coro, response, callback_id))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.remove)

    def _execute_steps(self):
        self._step_manager.start()

        for step in self.saga_process["steps"]:

            for operation in step:
                if operation["type"] == "withCompensation":
                    self.saga_process["current_compensations"].insert(0, operation)

            for operation in step:
                func = operation["method"]

                if operation["type"] == "invokeParticipant":
                    self._step_manager.operation(operation["id"], operation["type"], operation["name"])

                    try:
                        self._response = func(operation)
                    except:
                        self._rollback()
                        raise Exception(
                            "Error performing step {step}.".format(
                                step=operation["name"]
                            )
                        )

                if operation["type"] == "onReply":
                    try:
                        self._response = func(operation)
                    except:
                        self._rollback()
                        raise Exception("Error performing onReply method.")

    def _rollback(self):
        for operation in self.saga_process["current_compensations"]:
            func = operation["method"]
            func(operation)

        self._step_manager.close()


"""
{
    'name': 'OrdersAdd',
    'id': '653291cd-118c-4bb8-b861-a6acaa0e8dc6',
    'steps': [
        [
            {
                'id': '2cab6001-2181-4357-bb36-5b9d921a3659',
                'type': 'invokeParticipant',
                'method': <bound method Saga._invokeParticipant of <minos.microservice.saga.saga.Saga object at 0x1048a5810>>,
                'name': 'CreateOrder',
                'callback': <function create_order_callback2 at 0x1047448c0>
            },
            {
                'id': '9ea388ec-cbb1-442d-898b-7850ee068d11',
                'type': 'withCompensation',
                'method': <bound method Saga._withCompensation of <minos.microservice.saga.saga.Saga object at 0x1048a5810>>,
                'name': 'DeleteOrder',
                'callback': <function delete_order_callback at 0x1047a2b90>
            },
            {
                'id': 'e8f5421a-00a1-48ae-8077-39c8e3566c40',
                'type': 'onReply',
                'method': <bound method Saga._onReply of <minos.microservice.saga.saga.Saga object at 0x1048a5810>>,
                'callback': <function create_ticket_on_reply_callback at 0x104722b90>
            }
        ],
        [
            {
                'id': '23d0ea92-c0e7-496c-859d-65d4065438ec',
                'type': 'invokeParticipant',
                'method': <bound method Saga._invokeParticipant of <minos.microservice.saga.saga.Saga object at 0x1048a5810>>,
                'name': 'CreateTicket',
                'callback': None
            },
            {
                'id': '5a8693aa-5d7d-47a6-8569-2d3d80dbbd2e',
                'type': 'onReply',
                'method': <bound method Saga._onReply of <minos.microservice.saga.saga.Saga object at 0x1048a5810>>,
                'callback': <function create_ticket_on_reply_callback at 0x104722b90>
            }
        ]
    ]
}
"""
