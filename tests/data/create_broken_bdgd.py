#!/usr/bin/env python3
"""
Create broken Mux Energia BDGD copies for scan and Etapa17 validation tests/training.

Regeneration:
    python tests/data/create_broken_bdgd.py --target both
    python tests/data/create_broken_bdgd.py --target both --force

Requires the committed zip:
    sample/raw/aneel/Mux_Energia_401_2024-12-31_V11_20250806-1857.gdb.zip
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
import pyogrio

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tests.data.bdgd_sample_paths import copy_clean_to_broken, ensure_clean_mux_gdb
from tests.data.scan_manifest_utils import (
    ETAPA17_MANIFEST_PATH,
    SCAN_MANIFEST_PATH,
    load_manifest,
)

LAYER_ALIASES = {
    "UCMT_tab": "UCMT_tab",
    "UCBT_tab": "UCBT_tab",
    "UGBT_tab": "UGBT_tab",
    "UGMT_tab": "UGMT_tab",
}


def _resolve_layer_name(table_name: str) -> str:
    return LAYER_ALIASES.get(table_name, table_name)


def _read_layer(gdb_path: Path, table_name: str) -> gpd.GeoDataFrame:
    layer = _resolve_layer_name(table_name)
    return gpd.read_file(gdb_path, layer=layer, engine="pyogrio")


def _write_layer(gdb_path: Path, table_name: str, gdf: gpd.GeoDataFrame) -> None:
    layer = _resolve_layer_name(table_name)
    pyogrio.write_dataframe(gdf, gdb_path, layer=layer, driver="OpenFileGDB")


def _locate_row_index(gdf: pd.DataFrame, record_key: dict[str, Any]) -> int:
    column = record_key["column"]
    value = record_key["value"]
    matches = gdf.index[gdf[column].astype(str) == str(value)].tolist()
    if not matches:
        raise KeyError(f"Record not found in column {column}={value!r}")
    return matches[0]


def _apply_value(gdf: pd.DataFrame, index: int, field: str, value: Any) -> None:
    if value is None:
        gdf[field] = gdf[field].astype(object)
        gdf.at[index, field] = None
        return
    if not pd.api.types.is_numeric_dtype(gdf[field]) and not isinstance(value, str):
        value = str(value)
    gdf.at[index, field] = value


def apply_manifest_entries(gdb_path: Path, entries: list[dict[str, Any]]) -> None:
    touched_tables: dict[str, gpd.GeoDataFrame] = {}
    sorted_entries = sorted(
        entries,
        key=lambda entry: (
            entry["table"],
            1 if entry.get("field") == entry.get("record_key", {}).get("column") else 0,
        ),
    )
    for entry in sorted_entries:
        if entry.get("patch_in_memory"):
            continue
        table = entry["table"]
        if table not in touched_tables:
            touched_tables[table] = _read_layer(gdb_path, table)
        gdf = touched_tables[table]
        index = _locate_row_index(gdf, entry["record_key"])
        inject_fields = entry.get("inject_fields")
        if inject_fields:
            for field, value in inject_fields.items():
                _apply_value(gdf, index, field, value)
        else:
            _apply_value(gdf, index, entry["field"], entry.get("inject_value"))

        for secondary in entry.get("secondary_injections", []):
            secondary_table = secondary["table"]
            if secondary_table not in touched_tables:
                touched_tables[secondary_table] = _read_layer(gdb_path, secondary_table)
            secondary_gdf = touched_tables[secondary_table]
            secondary_index = _locate_row_index(secondary_gdf, secondary["record_key"])
            for field, value in secondary["inject_fields"].items():
                _apply_value(secondary_gdf, secondary_index, field, value)

    for table, gdf in touched_tables.items():
        _write_layer(gdb_path, table, gdf)


def create_scan_broken_gdb(*, force: bool = False) -> Path:
    gdb_path = copy_clean_to_broken("scan", force=force)
    entries = load_manifest(SCAN_MANIFEST_PATH)
    apply_manifest_entries(gdb_path, entries)
    return gdb_path


def create_etapa17_broken_gdb(*, force: bool = False) -> Path:
    gdb_path = copy_clean_to_broken("etapa17", force=force)
    entries = load_manifest(ETAPA17_MANIFEST_PATH)
    apply_manifest_entries(gdb_path, entries)
    return gdb_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target",
        choices=["scan", "etapa17", "both"],
        default="both",
        help="Which broken GDB to build",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild the broken GDB even if it already exists",
    )
    args = parser.parse_args()

    ensure_clean_mux_gdb()
    if args.target in {"scan", "both"}:
        path = create_scan_broken_gdb(force=args.force)
        print(f"Created scan broken BDGD: {path}")
    if args.target in {"etapa17", "both"}:
        path = create_etapa17_broken_gdb(force=args.force)
        print(f"Created etapa17 broken BDGD: {path}")


if __name__ == "__main__":
    main()
