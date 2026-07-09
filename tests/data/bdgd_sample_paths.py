"""Paths and helpers for the Mux Energia BDGD test/training samples."""

from __future__ import annotations

import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SAMPLE_DIR = PROJECT_ROOT / "sample" / "raw" / "aneel"

MUX_ZIP_NAME = "Mux_Energia_401_2024-12-31_V11_20250806-1857.gdb.zip"
MUX_CLEAN_GDB_NAME = "Mux_Energia_401_2024-12-31_V11_20250806-1857.gdb"
MUX_BROKEN_SCAN_GDB_NAME = "Mux_Energia_401_broken_scan_for_tests.gdb"
MUX_BROKEN_ETAPA17_GDB_NAME = "Mux_Energia_401_broken_etapa17_for_tests.gdb"

CREATE_SCRIPT = PROJECT_ROOT / "tests" / "data" / "create_broken_bdgd.py"

DATA_DIR = Path(__file__).resolve().parent
BROKEN_SCAN_MANIFEST = DATA_DIR / "broken_bdgd_scan_manifest.yml"
BROKEN_ETAPA17_MANIFEST = DATA_DIR / "broken_bdgd_etapa17_manifest.yml"


def mux_zip_path() -> Path:
    return SAMPLE_DIR / MUX_ZIP_NAME


def mux_clean_gdb_path() -> Path:
    return SAMPLE_DIR / MUX_CLEAN_GDB_NAME


def broken_gdb_path(phase: str) -> Path:
    if phase == "scan":
        return SAMPLE_DIR / MUX_BROKEN_SCAN_GDB_NAME
    if phase == "etapa17":
        return SAMPLE_DIR / MUX_BROKEN_ETAPA17_GDB_NAME
    raise ValueError(f"Unknown broken BDGD phase: {phase}")


def _manifest_path(phase: str) -> Path:
    if phase == "scan":
        return BROKEN_SCAN_MANIFEST
    if phase == "etapa17":
        return BROKEN_ETAPA17_MANIFEST
    raise ValueError(f"Unknown broken BDGD phase: {phase}")


def _remove_dir_if_exists(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def remove_mux_gdb_artifacts() -> None:
    """Delete the clean and broken Mux Energia GDB folders used by tests."""
    _remove_dir_if_exists(mux_clean_gdb_path())
    _remove_dir_if_exists(broken_gdb_path("scan"))
    _remove_dir_if_exists(broken_gdb_path("etapa17"))


def ensure_clean_mux_gdb(*, force: bool = False) -> Path:
    """Extract the committed zip into sample/raw/aneel.

    When ``force`` is True, any existing clean GDB folder is deleted first and
    the zip is extracted again.
    """
    clean_path = mux_clean_gdb_path()
    if force:
        _remove_dir_if_exists(clean_path)
    elif clean_path.exists():
        return clean_path

    zip_path = mux_zip_path()
    if not zip_path.exists():
        raise FileNotFoundError(
            f"Mux Energia sample zip not found: {zip_path}. "
            "Add the committed .gdb.zip before running integration tests."
        )

    with zipfile.ZipFile(zip_path, "r") as archive:
        archive.extractall(SAMPLE_DIR)

    nested = clean_path / clean_path.name
    if nested.exists():
        return nested
    if not clean_path.exists():
        raise FileNotFoundError(
            f"Zip extracted but expected GDB folder was not found: {clean_path}"
        )
    return clean_path


def ensure_broken_mux_gdb(phase: str, *, force: bool = False) -> Path:
    """Ensure the broken scan or etapa17 GDB exists, generating it when needed.

    When ``force`` is True, recreate from a fresh clean GDB even if the broken
    GDB already exists.
    """
    ensure_clean_mux_gdb(force=False)
    target = broken_gdb_path(phase)
    if target.exists() and not force:
        return target

    cmd = [sys.executable, str(CREATE_SCRIPT), "--target", phase, "--force"]
    subprocess.run(cmd, check=True, cwd=PROJECT_ROOT)
    if not target.exists():
        raise FileNotFoundError(f"Broken BDGD was not created: {target}")
    return target


def refresh_mux_gdbs_for_tests() -> dict[str, Path]:
    """Delete, re-unzip, and rebuild all Mux Energia GDB assets used by tests.

    Returns paths for the clean, scan-broken, and etapa17-broken GDBs.
    """
    remove_mux_gdb_artifacts()
    clean = ensure_clean_mux_gdb(force=True)
    scan = ensure_broken_mux_gdb("scan", force=True)
    etapa17 = ensure_broken_mux_gdb("etapa17", force=True)
    return {"clean": clean, "scan": scan, "etapa17": etapa17}


def copy_clean_to_broken(phase: str, *, force: bool = False) -> Path:
    """Copy the clean GDB directory to the broken GDB target path."""
    clean_path = ensure_clean_mux_gdb()
    target = broken_gdb_path(phase)
    if target.exists():
        if force:
            shutil.rmtree(target)
        else:
            return target
    shutil.copytree(clean_path, target)
    return target
