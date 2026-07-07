#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""Synthetic unit tests for scan type-check branches."""

import pandas as pd
import pytest

from bdgd2opendss.core.JsonData import Table
from bdgd2opendss.model.validador_bdgd import ValidadorBDGD


def _scan_errors_for_column(column: str, rule_type: str, value) -> pd.DataFrame:
    gdf = pd.DataFrame(
        {
            "COD_ID": ["X1"],
            column: [value],
        }
    )
    table = Table("TEST", ["COD_ID", column], {column: rule_type}, False)
    validator = ValidadorBDGD(df={}, tables={}, output_folder=".")
    validator.cod_base = "TEST"
    errors = validator._scan_table_columns("TEST", table, gdf, [], [], [], [])
    return pd.DataFrame(errors)


def test_scan_int_branch_rejects_non_integer_value():
    errors = _scan_errors_for_column("DIST", "int", "5")
    assert any("número inteiro" in err for err in errors["erro"])


def test_scan_float_branch_rejects_non_float_value():
    errors = _scan_errors_for_column("R1", "float", 5)
    assert any("deve ser um número." in err for err in errors["erro"])


def test_scan_string_branch_rejects_non_string_value():
    errors = _scan_errors_for_column("COD_ID", "string", 123)
    assert any("deve ser uma string" in err for err in errors["erro"])
