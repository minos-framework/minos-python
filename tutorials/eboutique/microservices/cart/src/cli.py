import logging
import sys
from pathlib import (
    Path,
)
from typing import (
    Optional,
)

import typer
from aiohttp import (
    web,
)
from ddtrace import (
    patch_all,
    tracer,
)
from ddtrace.contrib.aiohttp import (
    trace_app,
)
from ddtrace.contrib.asyncio import (
    context_provider,
)
from ddtrace.profiling import (
    Profiler,
)
from minos.common import (
    EntrypointLauncher,
    MinosConfig,
)
from minos.networks import (
    RestService,
)

patch_all(logging=True, aiohttp=True)

tracer.configure(hostname="datadog", port=8126, enabled=True, context_provider=context_provider)

prof = Profiler(service="cart", tracer=tracer)
prof.start()

logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
FORMAT = (
    "%(asctime)s %(levelname)s [%(name)s] [%(filename)s:%(lineno)d] "
    "[dd.service=%(dd.service)s dd.env=%(dd.env)s dd.version=%(dd.version)s dd.trace_id=%(dd.trace_id)s dd.span_id=%(dd.span_id)s] "
    "- %(message)s"
)
logging.basicConfig(format=FORMAT)
log = logging.getLogger(__name__)
log.level = logging.INFO

app = typer.Typer()


@app.command("start")
def start(
    file_path: Optional[Path] = typer.Argument(
        "config.yml",
        help="Microservice configuration file.",
        envvar="MINOS_CONFIGURATION_FILE_PATH",
    )
):
    """Start the microservice."""
    launcher = EntrypointLauncher.from_config(file_path, external_modules=[sys.modules["src"]])
    launcher.loop.run_until_complete(launcher.setup())
    rest_service: RestService = launcher.services[1]

    rest_app: web.Application = rest_service.handler.get_app()

    trace_app(rest_app, tracer, service="cart")
    launcher.launch()


@app.callback()
def callback():
    """Minos microservice CLI."""


def main():  # pragma: no cover
    """CLI's main function."""
    app()
