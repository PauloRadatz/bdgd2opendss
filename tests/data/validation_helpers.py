"""Shared helpers for BDGD validation integration tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from bdgd2opendss.config.paths import bdgd2dss_error_json
from bdgd2opendss.core.JsonData import JsonData
from bdgd2opendss.model.validador_bdgd import ValidadorBDGD


def load_validation_geodataframes(gdb_path: Path) -> tuple[dict, dict]:
    json_obj = JsonData(bdgd2dss_error_json)
    geodataframe, tables = json_obj.create_geodataframe_errors(str(gdb_path))
    for key in ("UCBT_tab", "UCMT_tab", "UGBT_tab", "UGMT_tab"):
        if key in geodataframe:
            geodataframe[key[:4]] = geodataframe.pop(key)
    return geodataframe, tables


def run_scan_validation(gdb_path: Path, output_folder: Path) -> pd.DataFrame:
    geodataframe, tables = load_validation_geodataframes(gdb_path)
    validator = ValidadorBDGD(
        df=geodataframe,
        tables=tables,
        output_folder=str(output_folder),
    )
    return validator.scan_bdgd()


def run_etapa17_validation(gdb_path: Path, output_folder: Path) -> pd.DataFrame:
    geodataframe, tables = load_validation_geodataframes(gdb_path)
    validator = ValidadorBDGD(
        df=geodataframe,
        tables=tables,
        output_folder=str(output_folder),
    )
    captured: list[pd.DataFrame] = []

    def capture_export(self, df, export_folder, feeder=""):
        captured.append(df.copy())

    original_export = ValidadorBDGD.exportar_erros_excel
    ValidadorBDGD.exportar_erros_excel = capture_export
    try:
        validator.run_validation()
    finally:
        ValidadorBDGD.exportar_erros_excel = original_export

    if not captured:
        return pd.DataFrame()
    return pd.concat(captured, ignore_index=True)


def assert_validation_message(
    df: pd.DataFrame,
    *,
    contains: str,
    table: str | None = None,
    codigo: str | None = None,
) -> None:
    if df.empty or "erro" not in df.columns:
        raise AssertionError(f"Expected message containing {contains!r}, but no errors were produced.")

    mask = df["erro"].astype(str).str.contains(contains, regex=False, na=False)
    if table is not None and "Tabela" in df.columns:
        mask &= df["Tabela"].astype(str) == table
    if codigo is not None and "Código" in df.columns:
        mask &= df["Código"].astype(str) == str(codigo)

    if not mask.any():
        sample = df.head(10).to_string()
        raise AssertionError(
            f"Expected validation message containing {contains!r}"
            f"{f' for table {table!r}' if table else ''}"
            f"{f' and codigo {codigo!r}' if codigo else ''}.\n"
            f"Sample errors:\n{sample}"
        )


def patch_scan_value_for_rule(
    gdf: pd.DataFrame,
    index: int,
    field: str,
    rule_kind: str,
    *,
    inject_value: Any = None,
) -> None:
    if inject_value is not None:
        gdf.at[index, field] = inject_value
        return
    if rule_kind == "float":
        gdf[field] = gdf[field].astype(object)
        gdf.at[index, field] = 5
    elif rule_kind == "int":
        gdf[field] = gdf[field].astype(object)
        gdf.at[index, field] = "5"
    elif rule_kind == "string":
        gdf[field] = gdf[field].astype(object)
        gdf.at[index, field] = 123
    else:
        raise ValueError(f"Unsupported in-memory patch rule: {rule_kind}")


def _lookup_geodataframe_key(table_name: str) -> str:
    if table_name.endswith("_tab"):
        return table_name[:4]
    return table_name


def _apply_manifest_entry(geodataframe: dict, case: dict[str, Any]) -> None:
    table_name = case["table"]
    lookup_name = _lookup_geodataframe_key(table_name)
    gdf = geodataframe[lookup_name]
    record_key = case["record_key"]
    matches = gdf.index[gdf[record_key["column"]].astype(str) == str(record_key["value"])].tolist()
    if not matches:
        raise KeyError(f"Record not found: {record_key}")
    index = matches[0]
    inject_fields = case.get("inject_fields")
    if inject_fields:
        for field, value in inject_fields.items():
            if value is None:
                gdf[field] = gdf[field].astype(object)
            gdf.at[index, field] = value
    else:
        value = case.get("inject_value")
        if value is None:
            gdf[case["field"]] = gdf[case["field"]].astype(object)
        gdf.at[index, case["field"]] = value

    for secondary in case.get("secondary_injections", []):
        secondary_lookup = _lookup_geodataframe_key(secondary["table"])
        secondary_gdf = geodataframe[secondary_lookup]
        secondary_key = secondary["record_key"]
        secondary_index = secondary_gdf.index[
            secondary_gdf[secondary_key["column"]].astype(str) == str(secondary_key["value"])
        ].tolist()[0]
        for field, value in secondary["inject_fields"].items():
            if value is None:
                secondary_gdf[field] = secondary_gdf[field].astype(object)
            secondary_gdf.at[secondary_index, field] = value


def run_patched_etapa17_case(
    gdb_path: Path,
    output_folder: Path,
    case: dict[str, Any],
) -> pd.DataFrame:
    geodataframe, tables = load_validation_geodataframes(gdb_path)
    geodataframe = {key: value.copy() for key, value in geodataframe.items()}
    _apply_manifest_entry(geodataframe, case)
    validator = ValidadorBDGD(
        df=geodataframe,
        tables=tables,
        output_folder=str(output_folder),
    )
    captured: list[pd.DataFrame] = []

    def capture_export(self, df, export_folder, feeder=""):
        captured.append(df.copy())

    original_export = ValidadorBDGD.exportar_erros_excel
    ValidadorBDGD.exportar_erros_excel = capture_export
    try:
        validator.run_validation()
    finally:
        ValidadorBDGD.exportar_erros_excel = original_export

    if not captured:
        return pd.DataFrame()
    return pd.concat(captured, ignore_index=True)


def run_patched_scan_case(
    gdb_path: Path,
    output_folder: Path,
    case: dict[str, Any],
) -> pd.DataFrame:
    import geopandas as gpd

    from bdgd2opendss.core.JsonData import Table

    geodataframe, tables = load_validation_geodataframes(gdb_path)
    table_name = case["table"]
    lookup_name = table_name
    if table_name.endswith("_tab"):
        lookup_name = table_name[:4]
    gdf = geodataframe[lookup_name].copy()
    record_key = case["record_key"]
    matches = gdf.index[gdf[record_key["column"]].astype(str) == str(record_key["value"])].tolist()
    if not matches:
        raise KeyError(f"Record not found: {record_key}")
    index = matches[0]
    patch_scan_value_for_rule(
        gdf,
        index,
        case["field"],
        case["rule_kind"],
        inject_value=case.get("inject_value"),
    )
    geodataframe[lookup_name] = gdf

    validator = ValidadorBDGD(
        df=geodataframe,
        tables=tables,
        output_folder=str(output_folder),
    )
    validator.cod_base = "401202412"
    table: Table = tables[table_name]
    return pd.DataFrame(
        validator._scan_table_columns(
            table_name,
            table,
            gdf.reset_index(drop=True),
            geodataframe["CTMT"]["COD_ID"].tolist(),
            geodataframe["SEGCON"]["COD_ID"].tolist(),
            geodataframe["UNTRMT"]["COD_ID"].tolist(),
            geodataframe["CRVCRG"]["COD_ID"].tolist(),
        )
    )
