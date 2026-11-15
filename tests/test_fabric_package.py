"""The Fabric Git-format package (agent/deployment/fabric_package.py, 2026-11-15): what a client deploys without this tool or the Fabric CLI.

The notebook format is checked against what Fabric itself returned from getDefinition on 2026-11-15 (NB_Param_Test, py_lib_test in
fabric-demo). Real code only; the package of the real project is built in tests/test_live.py."""

from __future__ import annotations

import json

from adf2fabric.agent.deployment import fabric_package as FP

NB = {"nbformat": 4, "nbformat_minor": 5, "metadata": {"language_info": {"name": "python"}}, "cells": [
    {"cell_type": "code", "metadata": {"tags": ["parameters"]}, "outputs": [], "execution_count": None,
     "source": ["# Parameters\n", "run_date = ''\n"]},
    {"cell_type": "markdown", "metadata": {}, "source": ["# Title\n", "\n", "text"]},
    {"cell_type": "code", "metadata": {}, "outputs": [], "execution_count": None, "source": "print(run_date)\n"}]}


def test_a_notebook_is_written_in_fabrics_own_source_format():
    src = FP.notebook_source(NB, None)
    assert src.startswith("# Fabric notebook source\n")
    assert "# PARAMETERS CELL ********************\n\n# Parameters\nrun_date = ''" in src  # as Fabric returned NB_Param_Test
    assert "# CELL ********************\n\nprint(run_date)" in src
    assert "# MARKDOWN ********************\n\n# # Title\n#\n# text" in src
    assert "METADATA" not in src  # nothing attached, so no META block


def test_an_attached_environment_is_a_meta_dependency_by_logical_id_in_any_workspace():
    src = FP.notebook_source(NB, "11111111-2222-3333-4444-555555555555")
    meta = "\n".join(line[len("# META "):] for line in src.splitlines() if line.startswith("# META "))
    assert json.loads(meta) == {"dependencies": {"environment": {"environmentId": "11111111-2222-3333-4444-555555555555", "workspaceId": FP.ZERO}}}
    assert src.index("# METADATA") < src.index("# PARAMETERS CELL")  # as Fabric returned py_lib_test: META first, then the cells


def test_platform_file_and_logical_ids():
    lid = FP.logical_id("p", "Notebook", "NB_A")
    assert lid == FP.logical_id("p", "Notebook", "nb_a")  # a rebuild keeps it, whatever the case
    assert lid != FP.logical_id("p", "DataPipeline", "NB_A") and lid != FP.logical_id("q", "Notebook", "NB_A")
    plat = json.loads(FP.platform("Notebook", "NB_A", lid, "Generated."))
    assert plat == {"$schema": FP.PLATFORM_SCHEMA, "metadata": {"type": "Notebook", "displayName": "NB_A", "description": "Generated."},
                    "config": {"version": "2.0", "logicalId": lid}}
