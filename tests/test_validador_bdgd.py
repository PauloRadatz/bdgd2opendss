#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""Tests for pandas 3 compatibility in BDGD validation."""

import pathlib
import tempfile
import zipfile

import pandas as pd
import pytest

import bdgd2opendss as bdgd
from bdgd2opendss.core.JsonData import Table
from bdgd2opendss.core.Utils import lookup_ctmt_by_pac, normalize_pac_columns, normalize_pac_value
from bdgd2opendss.model.validador_bdgd import (
    ValidadorBDGD,
    _report_verification,
    _verification_log_path,
)


def _minimal_validation_df():
    empty_line = pd.DataFrame(columns=["COD_ID", "CTMT", "PAC_1", "PAC_2"])
    return {
        "BASE": pd.DataFrame({"DIST": ["598"], "DAT_EXT": ["31/12/2023"]}),
        "CTMT": pd.DataFrame({"COD_ID": ["F1"], "PAC_INI": ["1"], "ATIP": [0]}),
        "EQTRMT": pd.DataFrame(columns=["COD_ID", "UNI_TR_MT"]),
        "UNTRMT": pd.DataFrame(columns=["COD_ID", "CTMT", "PAC_1", "PAC_2", "UNI_TR_MT"]),
        "EQRE": pd.DataFrame(columns=["COD_ID", "UN_RE"]),
        "UNREMT": pd.DataFrame(columns=["COD_ID", "CTMT", "PAC_1", "PAC_2", "UN_RE"]),
        "SSDMT": empty_line.copy(),
        "SSDBT": empty_line.copy(),
        "RAMLIG": empty_line.copy(),
        "UNSEMT": pd.DataFrame(columns=["COD_ID", "CTMT", "PAC_1", "PAC_2", "P_N_OPE"]),
        "UNSEBT": empty_line.copy(),
        "UCMT": pd.DataFrame(columns=["COD_ID", "CTMT", "PAC"]),
        "UCBT": pd.DataFrame(columns=["COD_ID", "CTMT", "PAC"]),
        "PIP": pd.DataFrame(columns=["COD_ID", "CTMT", "PAC"]),
        "UGBT": pd.DataFrame(columns=["CEG_GD", "CTMT", "PAC"]),
        "UGMT": pd.DataFrame(columns=["CEG_GD", "CTMT", "PAC"]),
    }


def _stub_heavy_validation_steps(monkeypatch):
    def empty_df(self, *args, **kwargs):
        return pd.DataFrame()

    def empty_isolados(self, *args, **kwargs):
        return pd.DataFrame(), pd.DataFrame()

    def empty_feeder(self, *args, **kwargs):
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    for name in (
        "check_ctmt", "check_lines", "check_unse", "check_transformer", "check_regulator",
        "check_ucmt", "check_energy", "check_loadbt", "check_faseamento", "check_propagacao",
        "check_voltage", "iso_trafo", "check_mrt", "phase_error", "bancos_trafos",
        "fase_df_da_problematico", "check_voltage_trafo", "bancos_regul",
        "fase_df_da_problematico_regul", "pac_iguais", "check_parallel",
    ):
        monkeypatch.setattr(ValidadorBDGD, name, empty_df)
    monkeypatch.setattr(ValidadorBDGD, "elem_isolados", empty_isolados)
    monkeypatch.setattr(ValidadorBDGD, "check_feeder", empty_feeder)


def get_creluz_d_path():
    project_root = pathlib.Path(__file__).parent.parent
    zip_path = project_root / "sample/raw/aneel/Creluz-D_598_2023-12-31_V11_20240715-1111.gdb.zip"
    gdb_dir = project_root / "sample/raw/aneel/Creluz-D_598_2023-12-31_V11_20240715-1111.gdb"

    if zip_path.exists() and not gdb_dir.exists():
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(gdb_dir.parent)

    if not gdb_dir.exists():
        return None

    nested_path = gdb_dir / gdb_dir.name
    return nested_path if nested_path.exists() else gdb_dir


class TestPacHelpers:
    def test_normalize_pac_value_handles_empty_and_nulo(self):
        assert pd.isna(normalize_pac_value(""))
        assert pd.isna(normalize_pac_value("Nulo"))
        assert normalize_pac_value(" 12345 ") == "12345"

    def test_lookup_ctmt_by_pac_returns_none_when_not_found(self):
        df = normalize_pac_columns(pd.DataFrame({
            "PAC_1": ["100"],
            "PAC_2": [pd.NA],
            "CTMT": ["F1"],
        }))
        assert lookup_ctmt_by_pac(df, "999") is None

    def test_lookup_ctmt_by_pac_matches_mixed_input_types(self):
        df = normalize_pac_columns(pd.DataFrame({
            "PAC_1": ["100"],
            "PAC_2": [pd.NA],
            "CTMT": ["F1"],
        }))
        assert lookup_ctmt_by_pac(df, 100) == "F1"


class TestCheckFeeder:
    def test_check_feeder_does_not_raise_with_mixed_pac_types(self):
        df = {
            "CTMT": pd.DataFrame({"COD_ID": ["F1"], "PAC_INI": ["1"]}),
            "SSDMT": pd.DataFrame({
                "COD_ID": ["L1"],
                "CTMT": ["F1"],
                "PAC_1": ["100"],
                "PAC_2": ["101"],
            }),
            "SSDBT": pd.DataFrame(columns=["COD_ID", "CTMT", "PAC_1", "PAC_2"]),
            "RAMLIG": pd.DataFrame(columns=["COD_ID", "CTMT", "PAC_1", "PAC_2"]),
            "UNSEMT": pd.DataFrame(columns=["COD_ID", "CTMT", "PAC_1", "PAC_2", "P_N_OPE"]),
            "UNSEBT": pd.DataFrame(columns=["COD_ID", "CTMT", "PAC_1", "PAC_2"]),
            "UCMT": pd.DataFrame(columns=["COD_ID", "CTMT", "PAC"]),
            "UCBT": pd.DataFrame(columns=["COD_ID", "CTMT", "PAC"]),
            "PIP": pd.DataFrame(columns=["COD_ID", "CTMT", "PAC"]),
            "UGBT": pd.DataFrame(columns=["CEG_GD", "CTMT", "PAC"]),
            "UGMT": pd.DataFrame(columns=["CEG_GD", "CTMT", "PAC"]),
        }
        df_isolados = pd.DataFrame({
            "COD_ID": ["ISO1"],
            "CTMT": ["F2"],
            "PAC_1": [100],
            "PAC_2": [""],
            "ELEM": ["SSDBT"],
        })
        df_trafo = pd.DataFrame(columns=["COD_ID", "CTMT", "PAC_1", "PAC_2", "BANC"])
        df_reg = pd.DataFrame(columns=["COD_ID", "CTMT", "PAC_1", "PAC_2", "UN_RE", "BANC"])

        validator = ValidadorBDGD(df=df, tables={}, output_folder=None)
        validator.cod_base = "TEST"

        erros, _, _ = validator.check_feeder(df, df_isolados, df_trafo, df_reg)

        assert isinstance(erros, pd.DataFrame)
        assert len(erros) == 1
        assert "F1" in erros.iloc[0]["detalhamento"]


class TestVerificationProgress:
    def test_report_verification_prints(self, capsys):
        _report_verification("teste", "mensagem de teste")
        captured = capsys.readouterr()
        assert "[verificacao] teste: mensagem de teste" in captured.out


class TestResilientVerification:
    def test_run_validation_continues_after_step_failure(self, monkeypatch, tmp_path):
        _stub_heavy_validation_steps(monkeypatch)

        def failing_pacs(self, *args, **kwargs):
            raise RuntimeError("test failure")

        monkeypatch.setattr(ValidadorBDGD, "check_pacs", failing_pacs)

        validator = ValidadorBDGD(df=_minimal_validation_df(), tables={}, output_folder=str(tmp_path))
        validator.run_validation()

        log_path = pathlib.Path(_verification_log_path(str(tmp_path), "", "598202312"))
        assert log_path.exists()
        assert "test failure" in log_path.read_text(encoding="utf-8")
        xlsx_files = list(tmp_path.glob("*_etapa17_*.xlsx"))
        assert len(xlsx_files) == 1

    def test_run_validation_concat_excludes_failed_steps(self, monkeypatch, tmp_path):
        _stub_heavy_validation_steps(monkeypatch)
        exported: list[pd.DataFrame] = []

        def capture_export(self, df, output_folder, feeder=""):
            exported.append(df)

        def success_row(self, *args, **kwargs):
            return pd.DataFrame([{
                "COD_BASE": "598202312",
                "Erro máx": "0%",
                "Tabela": "CTMT",
                "Código": "X",
                "erro": "test success",
                "detalhamento": "ok",
            }])

        def failing_pacs(self, *args, **kwargs):
            raise RuntimeError("test failure")

        monkeypatch.setattr(ValidadorBDGD, "check_ctmt_energy", success_row)
        monkeypatch.setattr(ValidadorBDGD, "check_pacs", failing_pacs)
        monkeypatch.setattr(ValidadorBDGD, "exportar_erros_excel", capture_export)

        validator = ValidadorBDGD(df=_minimal_validation_df(), tables={}, output_folder=str(tmp_path))
        validator.run_validation()

        assert len(exported) == 1
        assert len(exported[0]) == 1
        assert exported[0].iloc[0]["erro"] == "test success"

    def test_scan_bdgd_continues_after_table_failure(self, tmp_path):
        tables = {
            "CTMT": Table("CTMT", ["COD_ID"], {"COD_ID": "string"}, False),
            "SSDMT": Table("SSDMT", ["COD_ID"], {"MISSING_COL": "string"}, False),
        }
        df = {
            "BASE": pd.DataFrame({"DIST": ["598"], "DAT_EXT": ["31/12/2023"]}),
            "CTMT": pd.DataFrame({"COD_ID": ["F1"]}),
            "SSDMT": pd.DataFrame({"COD_ID": ["L1"]}),
        }
        validator = ValidadorBDGD(df=df, tables=tables, output_folder=str(tmp_path))
        validator.scan_bdgd()

        assert list(tmp_path.glob("*_scan_*.xlsx"))
        log_path = pathlib.Path(_verification_log_path(str(tmp_path), "", "598202312"))
        assert log_path.exists()
        assert "MISSING_COL" in log_path.read_text(encoding="utf-8")


class TestVerificacaoBdgd:
    def test_verificacao_bdgd_creluz_sample(self):
        sample_path = get_creluz_d_path()
        if sample_path is None:
            pytest.skip("Creluz-D sample not found")

        with tempfile.TemporaryDirectory() as tmpdir:
            bdgd.verificacao_bdgd(str(sample_path), True, None, tmpdir)
