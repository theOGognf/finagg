# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import shutil
import sys

# -- Path setup --------------------------------------------------------------

__location__ = os.path.dirname(__file__)

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
sys.path.insert(0, os.path.join(__location__, "../src"))

# -- Run sphinx-apidoc -------------------------------------------------------
# This hack is necessary since RTD does not issue `sphinx-apidoc` before running
# `sphinx-build -b html . _build/html`. See Issue:
# https://github.com/readthedocs/readthedocs.org/issues/1139
# DON'T FORGET: Check the box "Install your project inside a virtualenv using
# setup.py install" in the RTD Advanced Settings.
# Additionally it helps us to avoid running apidoc manually

try:  # for Sphinx >= 1.7
    from sphinx.ext import apidoc
except ImportError:
    from sphinx import apidoc

output_dir = os.path.join(__location__, "api")
module_dir = os.path.join(__location__, "../src/finagg")
try:
    shutil.rmtree(output_dir)
except FileNotFoundError:
    pass

try:
    import sphinx

    cmd_line = f"sphinx-apidoc --implicit-namespaces -f -o {output_dir} {module_dir}"

    args = cmd_line.split(" ")
    if tuple(sphinx.__version__.split(".")) >= ("1", "7"):
        # This is a rudimentary parse_version to avoid external dependencies
        args = args[1:]

    apidoc.main(args)
except Exception as e:
    print("Running `sphinx-apidoc` failed!\n{}".format(e))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "finagg"
copyright = "Andrew B"
author = "Andrew B"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# Add any Sphinx extension module names here, as strings. They can be extensions
# coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autosummary",
    "sphinx.ext.viewcode",
    "sphinx.ext.coverage",
    "sphinx.ext.doctest",
    "sphinx.ext.ifconfig",
    "sphinx.ext.mathjax",
    "sphinx.ext.napoleon",
    "sphinx.ext.extlinks",
    "sphinx_copybutton",
    "sphinx_rtd_theme",
    "sphinxcontrib.programoutput",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Autodoc options ---------------------------------------------------------

autodoc_member_order = "bysource"

# -- doctest options ---------------------------------------------------------

doctest_global_setup = """
import finagg
import pandas as pd
pd.set_option("display.precision", 4)
"""

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_css_files = [
    "custom.css",  # Avoid name clashes with the theme
]

# -- External mapping --------------------------------------------------------

python_version = ".".join(map(str, sys.version_info[0:2]))
intersphinx_mapping = {
    "python": ("https://docs.python.org/" + python_version, None),
    "numpy": ("https://numpy.org/doc/stable", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable", None),
}
