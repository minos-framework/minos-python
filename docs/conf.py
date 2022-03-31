import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(".."))

from minos import common

exclude_patterns = [
    "standard/docs/*.md",  # FIXME: Include these directories.
    "standard/docs/architecture/*.md",  # FIXME: Include these directories.
    "_build",
    "Thumbs.db",
    ".DS_Store",
]

extensions = [
    # "sphinxcontrib.apidoc",
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autosummary",
    "m2r2",
]
templates_path = ["_templates"]
source_suffix = [".rst", ".md"]

master_doc = "index"
project = "minos-python"
copyright = f"2021-{datetime.now().year}, Clariteia"
author = "Minos Framework Devs"

version = common.__version__
release = common.__version__

html_theme = "sphinx_rtd_theme"
html_sidebars = {"**": ["about.html", "navigation.html", "searchbox.html"]}
html_static_path = ["_static"]

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "inherited-members": True,
    "show-inheritance": True,
    "member-order": "bysource",
}
autodoc_mock_imports = [
    "unittest",
]

autoclass_content = "class"
autodoc_class_signature = "separated"
autodoc_member_order = "bysource"
autodoc_typehints_format = "short"
autodoc_typehints = "description"
autodoc_preserve_defaults = True
add_module_names = False
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}
autosummary_generate = True
