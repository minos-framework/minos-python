from __future__ import annotations

from pathlib import Path
from unittest import IsolatedAsyncioTestCase

import yaml
from cached_property import cached_property
from pytest_kind import KindCluster

DOCKER_COMPOSE_YML = "/home/clariteia/PycharmProjects/minos-python/tutorials/eboutique/docker-compose.yml"


class MinosTestCase(IsolatedAsyncioTestCase):
    @cached_property
    def services(self) -> dict | None:
        with Path(DOCKER_COMPOSE_YML).open() as dc_file:
            dc = yaml.safe_load(dc_file)
        if "services" in dc:
            services = dc["services"]
        else:
            services = None

        return services

    @cached_property
    def images(self):
        images = {}
        for name, description in self.services.items():
            images[name] = description["image"] if "image" in description else None

        return images

    def setUp(self) -> None:
        self.kind_cluster = KindCluster(
            "minos-testbench",
            None,
        )
        self.kind_cluster.create()
        self.load_services()

    def tearDown(self) -> None:
        pass
        # self.kind_cluster.delete()

    def load_services(self) -> None:
        self.kind_cluster.load_docker_image("postgres:minos")
        self.kind_cluster.kubectl("apply", "-f",
                                  "/home/clariteia/PycharmProjects/minos-python/tutorials/"
                                  "eboutique/microservices/product/tests/deployment.yaml")
        # for _, image in self.images.items():
        #     self.kind_cluster.load_docker_image(image)
