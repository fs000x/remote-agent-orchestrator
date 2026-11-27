"""RAO — Remote Agent Orchestrator.

Single source of truth for the package version: pyproject.toml reads it via
[tool.setuptools.dynamic], the CLI and the master node protocol read it here.
Bump ONLY this value when releasing.
"""
__version__ = "0.11.1"
