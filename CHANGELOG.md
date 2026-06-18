# Changelog

## 1.2.0 (2026-06-18)

- **xlsxwriter**: Added as a runtime dependency for BDGD validation Excel export.
- **Public API**: Package autocomplete now exposes only `run`, `verificacao_bdgd`, `bdgd_type`, `get_feeder_list`, `settings`, and `__version__`.
- **bdgd_type**: Now returns `"privada"` or `"publica"` based on the BDGD file format.
- **pandas 3 / Python 3.14**: `verificacao_bdgd` migrated for pandas 3.0 (PAC normalization, CoW-safe DataFrame updates, Windows console encoding, null-safe enum/PAC checks via `pd.isna`).
- **verificacao_bdgd**: Progress messages and feeder `tqdm` bars during validation.
- **verificacao_bdgd**: Fault-tolerant validation; step failures are logged to `*_verificacao_*.log` and Excel export includes results from completed steps.

## 1.1.0 (2026-03-25)

- **Python 3.14**: Documented and supported via `Programming Language :: Python :: 3.14` and dedicated dependency pins in `pyproject.toml` (see `requirements_py314.txt`).

## 1.0.0 (2024-08-16)

- First version coming from bdgd-tools.
