#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""Unit tests for bdgd_type and get_feeder_list."""

import pathlib
import zipfile

import pytest

import bdgd2opendss as bdgd
from bdgd2opendss import settings

CRELUZ_D_FEEDERS = [
    "17_SE001_1",
    "18_SE001_1",
    "1_3PAS_1",
    "1_ALPE_1",
    "1_BOES_1",
    "1_MIRA_1",
    "1_REDE2_1",
    "1_SE001_1",
    "22_SE001_1",
    "25_SE001_1",
    "2_PAL2_1",
    "30_SE001_1",
    "9718_9718_1",
]


@pytest.fixture
def tipo_bdgd_default():
    original = settings._TipoBDGD
    settings._TipoBDGD = False
    yield
    settings._TipoBDGD = original


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


class TestBdgdType:
    def test_detects_private_bdgd_from_shapefile_extensions(self, tmp_path, tipo_bdgd_default):
        (tmp_path / "layer.dbf").touch()
        assert bdgd.bdgd_type(str(tmp_path)) == "privada"
        assert settings._TipoBDGD is True

    def test_detects_public_bdgd_from_gdb_extensions(self, tmp_path, tipo_bdgd_default):
        (tmp_path / "a00000001.gdbtable").touch()
        assert bdgd.bdgd_type(str(tmp_path)) == "publica"
        assert settings._TipoBDGD is False

    def test_leaves_default_when_no_recognized_extensions(self, tmp_path, tipo_bdgd_default):
        (tmp_path / "readme.txt").touch()
        assert bdgd.bdgd_type(str(tmp_path)) is None
        assert settings._TipoBDGD is False


class TestGetFeederList:
    def test_creluz_d_feeder_list(self, tipo_bdgd_default):
        sample_path = get_creluz_d_path()
        if sample_path is None:
            pytest.skip("Creluz-D sample not found")

        assert bdgd.bdgd_type(str(sample_path)) == "publica"
        assert bdgd.get_feeder_list(str(sample_path)) == CRELUZ_D_FEEDERS
