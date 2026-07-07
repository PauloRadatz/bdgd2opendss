# Changelog

## 1.2.4 (2026-07-07)

- **Tests**: Added broken-BDGD integration pytest coverage for scan and Etapa17 validation, with manifest-driven GDB injections and in-memory patches where GDB edits are unreliable.
- **Tests**: Hardened Etapa17 `faseamento_mismatch` and `propagacao_mismatch` injections using degree-2 topology so results are stable across `networkx` versions.
- **Tests**: Excluded `UNTRAT` from scan validation manifest and test seeding.
- **Tests**: Broken GDB fixtures now regenerate automatically when the manifest or `create_broken_bdgd.py` is newer than the on-disk GDB.
- **Dependencies**: Aligned the Python 3.11–3.13 data/geo stack with 3.14 (`pandas==3.0.1`, `numpy==2.4.3`, `networkx==3.6.1`, `geopandas==1.1.3`, etc.); pandas below 3.0 reads empty PAC fields as `None`, which crashed `check_faseamento` in `networkx`.
- **Dependencies**: Resolved Python 3.12 `PyYAML` pin conflict (`6.0.1` → `6.0.3`) for `pip`/`tox` installs; fixed `requirements_py312.txt` UTF-8 encoding.
- **tox**: Default testenv and `flake8` env use Python 3.14; added `skip_missing_interpreters`; added `.python-version` (`3.14`) for IDE defaults.

## 1.2.3 (2026-07-05)

- **run** / **verificacao_bdgd**: Missing optional BDGD tables `UNSEBT` and `UNTRAT` no longer abort loading; an empty table is used and a warning is printed instead.
- **JsonData**: Consolidated BDGD layer loading into a shared `_read_layer()` helper used by both `create_geodataframes()` (`run`) and `create_geodataframe_errors()` (`verificacao_bdgd`).

## 1.2.2 (2026-07-03)

- **Breaking change**: `output_folder` is now required for `run` and `verificacao_bdgd`; implicit writes to `dss_models_output` and `dss_validation` under the current working directory were removed.
- **verificacao_bdgd**: Replaced `path_coords` with `export_figs` to optionally generate `buscoords.csv` for validation plots.
- **verificacao_bdgd**: Added Plotly plots for phase-wiring and isolated-element validation errors.
- **plotly**: Added as a runtime dependency.
- **BusCoords**: Improved coordinate generation for consumer units (UCs) and fixed public BDGD compatibility issues.

## 1.2.1 (2026-06-22)

- **Transformer**: Fixed 3-winding center-tapped transformers so the 3rd winding bus keeps the correct node suffix (e.g. `.4.2`) instead of dropping to the base bus name, which had left secondary legs isolated and prevented OpenDSS from solving the model.

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
