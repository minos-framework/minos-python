import os
import sys

sys.path.insert(0, os.path.abspath('..'))

import sphinx_rtd_theme  # noqa

extensions = [
    "sphinx.ext.viewcode",
    "sphinx_rtd_theme",
    "m2r2",
]

source_suffix = ['.rst', '.md']

master_doc = 'index'

project = 'Minos Python'
copyright = "2021, Clariteia"
author = "Minos Framework Devs"

language = None

templates_path = ['_templates']

exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

pygments_style = 'sphinx'

todo_include_todos = False

html_theme = 'sphinx_rtd_theme'

html_extra_path = ['api-reference']

html_sidebars = {"**": ["about.html", "navigation.html", "searchbox.html"]}

html_static_path = ["_static"]
