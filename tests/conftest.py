"""Shared pytest fixtures for broken BDGD validation integration tests."""

from __future__ import annotations

import pytest

from tests.data.bdgd_sample_paths import ensure_broken_mux_gdb, ensure_clean_mux_gdb, mux_zip_path
from tests.data.scan_manifest_utils import (
    ETAPA17_MANIFEST_PATH,
    SCAN_MANIFEST_PATH,
    load_manifest,
    validate_scan_manifest_completeness,
)
from tests.data.validation_helpers import run_etapa17_validation, run_scan_validation


@pytest.fixture(scope="session")
def mux_zip_available() -> None:
    if not mux_zip_path().exists():
        pytest.skip(f"Mux Energia zip not found: {mux_zip_path()}")


@pytest.fixture(scope="session")
def clean_mux_gdb(mux_zip_available):
    return ensure_clean_mux_gdb()


@pytest.fixture(scope="session")
def broken_scan_gdb(mux_zip_available, tmp_path_factory):
    return ensure_broken_mux_gdb("scan")


@pytest.fixture(scope="session")
def broken_etapa17_gdb(mux_zip_available, tmp_path_factory):
    return ensure_broken_mux_gdb("etapa17")


@pytest.fixture(scope="session")
def scan_manifest_entries():
    missing = validate_scan_manifest_completeness(SCAN_MANIFEST_PATH)
    assert not missing, f"Scan manifest missing JSON rules: {missing}"
    return load_manifest(SCAN_MANIFEST_PATH)


@pytest.fixture(scope="session")
def etapa17_manifest_entries():
    return load_manifest(ETAPA17_MANIFEST_PATH)


@pytest.fixture(scope="session")
def scan_errors(broken_scan_gdb, tmp_path_factory):
    output_folder = tmp_path_factory.mktemp("scan_validation_output")
    return run_scan_validation(broken_scan_gdb, output_folder)


@pytest.fixture(scope="session")
def etapa17_errors(broken_etapa17_gdb, tmp_path_factory):
    output_folder = tmp_path_factory.mktemp("etapa17_validation_output")
    return run_etapa17_validation(broken_etapa17_gdb, output_folder)
