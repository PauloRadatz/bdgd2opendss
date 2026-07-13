#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""Golden-file tests for generated csv_files outputs."""

import pathlib
import tempfile
import warnings
import zipfile

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

import bdgd2opendss as bdgd
from bdgd2opendss import settings
from bdgd2opendss.core import Utils
from bdgd2opendss.model import Load as load_module


FEEDER = "1_3PAS_1"
EXPECTED_CSV_FILES = {
    f"AuxCargaBT_{FEEDER}.csv",
    f"AuxCargaBTNT_{FEEDER}.csv",
    f"AuxCargaMT_{FEEDER}.csv",
    f"AuxCargaMTNT_{FEEDER}.csv",
    f"CircMT_{FEEDER}.csv",
    f"contagem_dias_{FEEDER}.csv",
}


def _project_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parent.parent


def _creluz_d_path() -> pathlib.Path | None:
    project_root = _project_root()
    zip_path = project_root / "sample/raw/aneel/Creluz-D_598_2023-12-31_V11_20240715-1111.gdb.zip"
    gdb_dir = project_root / "sample/raw/aneel/Creluz-D_598_2023-12-31_V11_20240715-1111.gdb"

    if zip_path.exists() and not gdb_dir.exists():
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(gdb_dir.parent)

    if not gdb_dir.exists():
        return None

    nested_path = gdb_dir / gdb_dir.name
    return nested_path if nested_path.exists() else gdb_dir


def _reset_generation_state() -> None:
    load_module.df_energ_load = pd.DataFrame()
    load_module.df_energ_loadmt = pd.DataFrame()
    load_module.df_dias = pd.DataFrame()
    Utils.cod_year_bdgd = []
    Utils.substation = ""


def _assert_csv_matches_expected(actual_path: pathlib.Path, expected_path: pathlib.Path) -> None:
    actual = pd.read_csv(actual_path, sep=",")
    expected = pd.read_csv(expected_path, sep=",")
    assert_frame_equal(actual, expected, check_dtype=False, atol=1e-9, rtol=1e-9)


def test_creluz_1_3pas_csv_files_match_golden_outputs():
    sample_path = _creluz_d_path()
    if sample_path is None:
        pytest.skip("Creluz-D sample not found")

    expected_dir = _project_root() / "tests" / "data" / "expected_csv_files" / FEEDER
    assert {path.name for path in expected_dir.glob("*.csv")} == EXPECTED_CSV_FILES

    original_settings = {
        "TabelaPT": settings.TabelaPT,
        "TipoBDGD": settings._TipoBDGD,
        "intAdequarModeloCarga": settings.intAdequarModeloCarga,
        "dblVPUMin": settings.dblVPUMin,
        "csv_separator": settings.csv_separator,
    }

    _reset_generation_state()
    try:
        settings.TabelaPT = True
        settings._TipoBDGD = False
        settings.intAdequarModeloCarga = 1
        settings.dblVPUMin = 0.6
        settings.csv_separator = ","

        with tempfile.TemporaryDirectory() as tmpdir:
            output_folder = pathlib.Path(tmpdir)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=RuntimeWarning)
                warnings.simplefilter("ignore", category=FutureWarning)
                bdgd.run(
                    bdgd_file_path=sample_path,
                    output_folder=output_folder,
                    all_feeders=False,
                    lst_feeders=[FEEDER],
                )

            generated_csv_files = sorted(output_folder.rglob("csv_files/*.csv"))
            assert {path.name for path in generated_csv_files} == EXPECTED_CSV_FILES
            assert not list(output_folder.rglob("elementos_isolados_*.log"))

            for actual_path in generated_csv_files:
                _assert_csv_matches_expected(actual_path, expected_dir / actual_path.name)
    finally:
        for key, value in original_settings.items():
            setattr(settings, key, value)
        _reset_generation_state()
