"""Helpers for scan manifest generation, validation, and expected messages."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from bdgd2opendss.config.paths import bdgd2dss_error_json

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCAN_MANIFEST_PATH = Path(__file__).resolve().parent / "broken_bdgd_scan_manifest.yml"
ETAPA17_MANIFEST_PATH = Path(__file__).resolve().parent / "broken_bdgd_etapa17_manifest.yml"

# Tables present in bdgd2dss_error.json but intentionally excluded from scan coverage.
SCAN_EXCLUDED_TABLES = frozenset({"UNTRAT"})

# Stable record keys in the clean Mux Energia 401 sample.
DEFAULT_RECORD_KEYS: dict[str, dict[str, Any]] = {
    "BASE": {"column": "DIST", "value": 401},
    "CTMT": {"column": "COD_ID", "value": "1_TAP2_1"},
    "SEGCON": {"column": "COD_ID", "value": "2"},
    "CRVCRG": {"column": "COD_ID", "value": "RES_1"},
    "SSDMT": {"column": "COD_ID", "value": "2656"},
    "UNSEMT": {"column": "COD_ID", "value": "1CF_1"},
    "UNTRMT": {"column": "COD_ID", "value": "1_19"},
    "EQTRMT": {"column": "UNI_TR_MT", "value": "1_19"},
    "UNREMT": {"column": "COD_ID", "value": "1_3000"},
    "EQRE": {"column": "UN_RE", "value": "2_3002"},
    "SSDBT": {"column": "COD_ID", "value": "6200"},
    "RAMLIG": {"column": "COD_ID", "value": "17568"},
    "UCMT_tab": {
        "column": "COD_ID",
        "value": "f26bfed326918116e0dc79566ef0427e9505e38d622633d2c2fa81aa9bee52d4",
    },
    "UCBT_tab": {
        "column": "COD_ID",
        "value": "cc1ceef34eeb9a89a42364a69271f952ed9f82e6da74c67542f198f18158da76",
    },
    "PIP": {"column": "COD_ID", "value": "4876"},
    "UGBT_tab": {"column": "CEG_GD", "value": "GD.RS.000.720.131"},
    "UGMT_tab": {"column": "CEG_GD", "value": "CGH.PH.RS.000177-5"},
}


def classify_rule_type(rule_type: Any) -> str:
    if isinstance(rule_type, list):
        return "enum_list"
    if rule_type == "category":
        return "category"
    if rule_type in {"int", "float", "string"}:
        return rule_type
    if isinstance(rule_type, str) and rule_type.startswith("["):
        return "range"
    return "other"


def expected_message_for_rule(column: str, rule_type: Any) -> str:
    kind = classify_rule_type(rule_type)
    if kind == "enum_list":
        return f"O atributo {column} possui valor não esperado"
    if kind == "category":
        return f"O atributo {column} possui valor não esperado"
    if kind == "int":
        return "O valor esperado deve ser um número inteiro"
    if kind == "float":
        return "O valor esperado deve ser um número."
    if kind == "string":
        return "O valor esperado deve ser uma string"
    if kind == "range":
        return f"O atributo {column} possui valor fora dos limites"
    raise ValueError(f"Unsupported rule type for {column}: {rule_type!r}")


def injection_value_for_rule(rule_type: Any) -> Any:
    kind = classify_rule_type(rule_type)
    if kind in {"enum_list", "category"}:
        return "__INVALID__"
    if kind == "range":
        return "99999"
    if kind == "string":
        return ""
    if kind in {"float", "int"}:
        return None
    raise ValueError(f"Unsupported rule type: {rule_type!r}")


def injected_problem_description(column: str, rule_type: Any) -> str:
    kind = classify_rule_type(rule_type)
    if kind == "enum_list":
        return f"Set {column} to an invalid enum value."
    if kind == "category":
        return f"Set {column} to an unknown reference id."
    if kind == "range":
        return f"Set {column} outside the allowed numeric code range."
    if kind == "string":
        return f"Clear {column} to an empty string."
    if kind == "float":
        return f"Set {column} to a non-float value during scan (patched in memory for float rules)."
    if kind == "int":
        return f"Set {column} to a non-integer value during scan (patched in memory for int rules)."
    return f"Inject invalid value into {column}."


def iter_scan_rules_from_json(json_path: Path | None = None) -> list[dict[str, Any]]:
    path = json_path or Path(bdgd2dss_error_json)
    data = json.loads(path.read_text(encoding="utf-8"))
    rules: list[dict[str, Any]] = []
    for table_name, table_def in data["configuration"]["tables"].items():
        if table_name in SCAN_EXCLUDED_TABLES:
            continue
        for column, rule_type in table_def.get("type", {}).items():
            rules.append(
                {
                    "table": table_name,
                    "field": column,
                    "rule_type": rule_type,
                    "rule_kind": classify_rule_type(rule_type),
                }
            )
    return rules


def build_scan_manifest_entry(table: str, field: str, rule_type: Any) -> dict[str, Any]:
    record_key = DEFAULT_RECORD_KEYS[table]
    entry_id = f"scan_{table.lower()}_{field.lower()}"
    patch_in_memory = classify_rule_type(rule_type) in {"float", "int"} or (
        table == "BASE" and field == "DAT_EXT"
    )
    return {
        "id": entry_id,
        "phase": "scan",
        "validation": "_scan_table_columns",
        "table": table,
        "field": field,
        "rule_kind": classify_rule_type(rule_type),
        "record_key": dict(record_key),
        "injected_problem": injected_problem_description(field, rule_type),
        "expected_message_contains": expected_message_for_rule(field, rule_type),
        "expected_table": table,
        "inject_value": injection_value_for_rule(rule_type),
        "patch_in_memory": patch_in_memory,
    }


def load_manifest(path: Path) -> list[dict[str, Any]]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def validate_scan_manifest_completeness(manifest_path: Path | None = None) -> list[str]:
    manifest_path = manifest_path or SCAN_MANIFEST_PATH
    manifest = load_manifest(manifest_path)
    manifest_keys = {(item["table"], item["field"]) for item in manifest}
    missing = []
    for rule in iter_scan_rules_from_json():
        key = (rule["table"], rule["field"])
        if key not in manifest_keys:
            missing.append(f"{rule['table']}.{rule['field']}")
    return missing


def write_scan_manifest(path: Path | None = None) -> Path:
    path = path or SCAN_MANIFEST_PATH
    entries = []
    for rule in iter_scan_rules_from_json():
        entries.append(
            build_scan_manifest_entry(rule["table"], rule["field"], rule["rule_type"])
        )
    path.write_text(
        yaml.safe_dump(entries, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    return path


if __name__ == "__main__":
    target = write_scan_manifest()
    missing = validate_scan_manifest_completeness(target)
    if missing:
        raise SystemExit(f"Manifest still missing rules: {missing}")
    print(f"Wrote {len(load_manifest(target))} scan manifest entries to {target}")
