# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

import typing as t
import asyncio
import uuid
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
            "steps": []
        }
        self._local_state.create(self.uuid, structure)

    def _update_master(self, step: str):
        self._state = self._local_state.load_state(self.uuid)
        self._state["current_step"] = step
        self._state["steps"].append(step)

        self._local_state.create(self.uuid, self._state)
        log.debug(self._state)

    def step(self, step_uuid: str, type: str, name: str = ""):
        structure = {
            "id": step_uuid,
            "type": type,
            "name": name,
            "response": ""
        }

        self._update_master(step_uuid)

        self._local_state.create(step_uuid, structure)
        log.debug(self._local_state.load_state(step_uuid))

    def add_response(self, step_uuid: str, response: str):
        self._state = self._local_state.load_state(step_uuid)
        self._state["response"] = response
        self._local_state.create(step_uuid, self._state)
        log.debug(self._local_state.load_state(step_uuid))

    def close(self):
        self._state = self._local_state.load_state(self.uuid)
        self._state["state"] = "Completed"
        self._local_state.create(self.uuid, self._state)
        # log.debug(self._state)
        self._local_state.delete_state(self.uuid)


def _invokeParticipant():
    # if operation["name"] == "CreateTicket":
    #    raise Exception("invokeParticipantTest exception")
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

    def _invokeParticipant(self, operation):
        self._response = _invokeParticipant()
        self._step_manager.add_response(operation["id"], self._response)

        if operation["callback"] is not None:
            func = operation["callback"]

            if asyncio.iscoroutine(func):
                self._create_task(func(self._response))
            else:
                self._response = func(self._response)
                self._step_manager.add_response(operation["id"], self._response)

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
                response = _withCompensation(compensation)
        else:
            response = _withCompensation(operation["name"])

        if operation["callback"] is not None:
            func = operation["callback"]

            if asyncio.iscoroutine(func):
                self._create_task(func(response))
            else:
                self._response = func(response)

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
        func = operation["callback"]

        if asyncio.iscoroutine(func):
            self._create_task(func)
        else:
            self._response = func(self._response)

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

    def _create_task(self, coro: t.Awaitable[t.Any]):
        task = self.loop.create_task(coro)
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
                    self._step_manager.step(operation["id"], operation["type"], operation["name"])

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
            log.debug(operation)
            func = operation["method"]
            func(operation)


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
        ],
        [
            {
                'id': '3d20ddb8-1424-4617-828d-7d1e6067a3f4', 'type': 'invokeParticipant', 'method': <bound method Saga._invokeParticipant of <minos.microservice.saga.saga.Saga object at 0x1048a5810>>, 'name': 'Shipping', 'callback': None}, {'id': '73d3ce5a-6d34-4e02-9926-1ba480abd7b6', 'type': 'withCompensation', 'method': <bound method Saga._withCompensation of <minos.microservice.saga.saga.Saga object at 0x1048a5810>>, 'name': ['Failed', 'BlockOrder'], 'callback': <function shipping_callback at 0x1047a2050>}, {'type': 'execute', 'method': <bound method Saga._execute of <minos.microservice.saga.saga.Saga object at 0x1048a5810>>}]], 'current_compensations': []}
"""
