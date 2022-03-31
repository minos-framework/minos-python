import os
import sys
from datetime import (
    datetime,
)

sys.path.insert(0, os.path.abspath(".."))

from minos import aggregate as package

extensions = [
    "sphinxcontrib.apidoc",
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autosummary",
    "m2r2",
]
templates_path = ["_templates"]
source_suffix = [".rst", ".md"]

master_doc = "index"
project = "minos-microservice-aggregate"
copyright = f"2021-{datetime.now().year}, Clariteia"
author = "Minos Framework Devs"

version = package.__version__
release = package.__version__

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "sphinx_rtd_theme"
html_sidebars = {"**": ["about.html", "navigation.html", "searchbox.html"]}
html_static_path = ["_static"]

apidoc_module_dir = "../minos"
apidoc_output_dir = "api"
apidoc_toc_file = False
apidoc_separate_modules = True
apidoc_extra_args = [
    "--force",
    "--implicit-namespaces",
    "--templatedir=_templates",
]
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "inherited-members": True,
    #"special-members": "__init__",
    "member-order": "bysource",
}
autoclass_content = "class"
autodoc_class_signature = "separated"
autodoc_member_order = "bysource"
autodoc_typehints_format = "short"
autodoc_typehints = "description"
autodoc_preserve_defaults = True
add_module_names = False
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "minos-microservice-common": (
        "https://minos-framework.github.io/minos-python/packages/core/minos-microservice-common",
        ("../../minos-microservice-common/docs/_build/html/objects.inv", None),
    ),
    "minos-microservice-networks": (
        "https://minos-framework.github.io/minos-python/packages/core/minos-microservice-networks",
        ("../../minos-microservice-networks/docs/_build/html/objects.inv", None),
    ),

}
