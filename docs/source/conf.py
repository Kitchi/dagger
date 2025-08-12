# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys
sys.path.insert(0, os.path.abspath('../../src'))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Dagger"
copyright = "2025 Srikrishna Sekhar"
author = "Srikrishna Sekhar"
release = "0.1"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

master_doc = "index"
default_role = "code"

extensions = ["sphinx.ext.autodoc", "sphinx.ext.autosummary", "pallets_sphinx_themes"]
autosummary_generate = True

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
    "member-order": "bysource",
    "preserve_defaults": True,
}

templates_path = ["_templates"]
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "click"
html_theme_options = {"index_sidebar_logo": True}
html_static_path = ["_static"]
html_logo = "_static/dagger_logo_real.png"
