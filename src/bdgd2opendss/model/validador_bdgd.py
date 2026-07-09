from typing import Callable, Optional, Union
import pathlib
from bdgd2opendss.core.JsonData import JsonData
from bdgd2opendss.core.Utils import (
    adapt_regulators_names,
    assign_elem,
    assign_single_pac_network,
    create_output_folder,
    inner_entities_tables,
    lookup_ctmt_by_pac,
    normalize_pac_columns,
)
from bdgd2opendss.model.Converter import convert_tpotaprt,convert_tten
import pandas as pd
import numpy as np
import geopandas as gpd
import networkx as nx
import os
import os.path
import traceback
from datetime import datetime
from tqdm import tqdm
import plotly.graph_objects as go

def _report_verification(step: str, message: str) -> None:
    print(f"[verificacao] {step}: {message}", flush=True)


def _is_missing(value) -> bool:
    return value is None or pd.isna(value)


def _pac_key(value) -> Optional[str]:
    if _is_missing(value):
        return None
    return str(value).lower()


def _is_undeclared_string(value) -> bool:
    return _is_missing(value) or not isinstance(value, str) or len(value) == 0


def _invalid_enum(value, allowed) -> bool:
    if _is_missing(value):
        return False
    return value not in allowed


def _invalid_or_missing_enum(value, allowed) -> bool:
    if _is_missing(value):
        return True
    return value not in allowed


def _verification_log_path(output_folder: str, feeder: Optional[str], cod_base: str) -> str:
    if feeder:
        filename = f"{feeder}_verificacao_{cod_base}.log"
    else:
        filename = f"verificacao_{cod_base}.log"
    return os.path.join(output_folder, filename)


def _log_verification_failure(
    log_path: str,
    phase: str,
    step: str,
    desc: str,
    exc: BaseException,
) -> None:
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    block = (
        f"\n{'=' * 72}\n"
        f"[{datetime.now().isoformat(timespec='seconds')}] FALHA\n"
        f"Fase: {phase}\n"
        f"Etapa: {step}\n"
        f"Descricao: {desc}\n"
        f"Excecao: {type(exc).__name__}: {exc}\n"
        f"Traceback:\n{traceback.format_exc()}"
        f"\n{'=' * 72}\n"
    )
    with open(log_path, "a", encoding="utf-8") as log_file:
        log_file.write(block)


class VerificadorFaseamentoTrafo:

    def __init__(self,tip_lig):
        self.padroes_corretos_df = self._carregar_padroes_corretos_deltafechado()
        self.padroes_corretos_da = self._carregar_padroes_corretos_deltaaberto()
        self.tip_lig = tip_lig

    def _carregar_padroes_corretos_deltafechado(self):
        """Carrega todos os padrões corretos baseados na sua lógica original"""
        return [
            # Grupo 1: Padrões com Var01 começando com A/AN
            ((('A', 'AN'), 'CN', 'AN', ('B', 'BN'), 'AB', 'XX', ('C', 'CN'), 'BC', 'XX'),),
            ((('A', 'AN'), 'CN', 'AN', ('B', 'BN'), 'BC', 'XX', ('C', 'CN'), 'AB', 'XX'),),
            ((('A', 'AN'), 'AN', 'BN', ('B', 'BN'), 'CA', 'XX', ('C', 'CN'), 'BC', 'XX'),),
            ((('A', 'AN'), 'AN', 'BN', ('B', 'BN'), 'BC', 'XX', ('C', 'CN'), 'CA', 'XX'),),
            ((('A', 'AN'), 'BN', 'CN', ('B', 'BN'), 'CA', 'XX', ('C', 'CN'), 'AB', 'XX'),),
            ((('A', 'AN'), 'BN', 'CN', ('B', 'BN'), 'AB', 'XX', ('C', 'CN'), 'CA', 'XX'),),

            # Grupo 2: Padrões com Var01 = A/AN e Var04 = C/CN
            ((('A', 'AN'), 'CN', 'AN', ('C', 'CN'), 'AB', 'XX', ('B', 'BN'), 'BC', 'XX'),),
            ((('A', 'AN'), 'CN', 'AN', ('C', 'CN'), 'BC', 'XX', ('B', 'BN'), 'AB', 'XX'),),
            ((('A', 'AN'), 'AN', 'BN', ('C', 'CN'), 'CA', 'XX', ('B', 'BN'), 'BC', 'XX'),),
            ((('A', 'AN'), 'AN', 'BN', ('C', 'CN'), 'BC', 'XX', ('B', 'BN'), 'CA', 'XX'),),
            ((('A', 'AN'), 'BN', 'CN', ('C', 'CN'), 'CA', 'XX', ('B', 'BN'), 'AB', 'XX'),),
            ((('A', 'AN'), 'BN', 'CN', ('C', 'CN'), 'AB', 'XX', ('B', 'BN'), 'CA', 'XX'),),

            # Grupo 3: Padrões com Var04 começando com A/AN
            ((('B', 'BN'), 'AB', 'XX', ('A', 'AN'), 'CN', 'AN', ('C', 'CN'), 'BC', 'XX'),),
            ((('B', 'BN'), 'BC', 'XX', ('A', 'AN'), 'CN', 'AN', ('C', 'CN'), 'AB', 'XX'),),
            ((('B', 'BN'), 'CA', 'XX', ('A', 'AN'), 'AN', 'BN', ('C', 'CN'), 'BC', 'XX'),),
            ((('B', 'BN'), 'BC', 'XX', ('A', 'AN'), 'AN', 'BN', ('C', 'CN'), 'CA', 'XX'),),
            ((('B', 'BN'), 'CA', 'XX', ('A', 'AN'), 'BN', 'CN', ('C', 'CN'), 'AB', 'XX'),),
            ((('B', 'BN'), 'AB', 'XX', ('A', 'AN'), 'BN', 'CN', ('C', 'CN'), 'CA', 'XX'),),

            # Grupo 4: Padrões com Var04 = A/AN e Var01 = C/CN
            ((('C', 'CN'), 'AB', 'XX', ('A', 'AN'), 'CN', 'AN', ('B', 'BN'), 'BC', 'XX'),),
            ((('C', 'CN'), 'BC', 'XX', ('A', 'AN'), 'CN', 'AN', ('B', 'BN'), 'AB', 'XX'),),
            ((('C', 'CN'), 'CA', 'XX', ('A', 'AN'), 'AN', 'BN', ('B', 'BN'), 'BC', 'XX'),),
            ((('C', 'CN'), 'BC', 'XX', ('A', 'AN'), 'AN', 'BN', ('B', 'BN'), 'CA', 'XX'),),
            ((('C', 'CN'), 'CA', 'XX', ('A', 'AN'), 'BN', 'CN', ('B', 'BN'), 'AB', 'XX'),),
            ((('C', 'CN'), 'AB', 'XX', ('A', 'AN'), 'BN', 'CN', ('B', 'BN'), 'CA', 'XX'),),

            # Grupo 5: Padrões com Var07 começando com A/AN
            ((('C', 'CN'), 'BC', 'XX', ('B', 'BN'), 'AB', 'XX', ('A', 'AN'), 'CN', 'AN'),),
            ((('C', 'CN'), 'AB', 'XX', ('B', 'BN'), 'BC', 'XX', ('A', 'AN'), 'CN', 'AN'),),
            ((('C', 'CN'), 'BC', 'XX', ('B', 'BN'), 'CA', 'XX', ('A', 'AN'), 'AN', 'BN'),),
            ((('C', 'CN'), 'CA', 'XX', ('B', 'BN'), 'BC', 'XX', ('A', 'AN'), 'AN', 'BN'),),
            ((('C', 'CN'), 'AB', 'XX', ('B', 'BN'), 'CA', 'XX', ('A', 'AN'), 'BN', 'CN'),),
            ((('C', 'CN'), 'CA', 'XX', ('B', 'BN'), 'AB', 'XX', ('A', 'AN'), 'BN', 'CN'),),

            # Grupo 6: Padrões com Var07 = A/AN e Var04 = C/CN
            ((('B', 'BN'), 'BC', 'XX', ('C', 'CN'), 'AB', 'XX', ('A', 'AN'), 'CN', 'AN'),),
            ((('B', 'BN'), 'AB', 'XX', ('C', 'CN'), 'BC', 'XX', ('A', 'AN'), 'CN', 'AN'),),
            ((('B', 'BN'), 'BC', 'XX', ('C', 'CN'), 'CA', 'XX', ('A', 'AN'), 'AN', 'BN'),),
            ((('B', 'BN'), 'CA', 'XX', ('C', 'CN'), 'BC', 'XX', ('A', 'AN'), 'AN', 'BN'),),
            ((('B', 'BN'), 'AB', 'XX', ('C', 'CN'), 'CA', 'XX', ('A', 'AN'), 'BN', 'CN'),),
            ((('B', 'BN'), 'CA', 'XX', ('C', 'CN'), 'AB', 'XX', ('A', 'AN'), 'BN', 'CN'),),

            # Grupo 7: Padrões com Var01 começando com B/BN
            ((('B', 'BN'), 'CN', 'AN', ('C', 'CN'), 'AB', 'XX', ('A', 'AN'), 'BC', 'XX'),),
            ((('B', 'BN'), 'CN', 'AN', ('C', 'CN'), 'BC', 'XX', ('A', 'AN'), 'AB', 'XX'),),
            ((('B', 'BN'), 'AN', 'BN', ('C', 'CN'), 'CA', 'XX', ('A', 'AN'), 'BC', 'XX'),),
            ((('B', 'BN'), 'AN', 'BN', ('C', 'CN'), 'BC', 'XX', ('A', 'AN'), 'CA', 'XX'),),
            ((('B', 'BN'), 'BN', 'CN', ('C', 'CN'), 'CA', 'XX', ('A', 'AN'), 'AB', 'XX'),),
            ((('B', 'BN'), 'BN', 'CN', ('C', 'CN'), 'AB', 'XX', ('A', 'AN'), 'CA', 'XX'),),

            # Grupo 8: Padrões com Var01 = B/BN e Var04 = A/AN
            ((('B', 'BN'), 'CN', 'AN', ('A', 'AN'), 'AB', 'XX', ('C', 'CN'), 'BC', 'XX'),),
            ((('B', 'BN'), 'CN', 'AN', ('A', 'AN'), 'BC', 'XX', ('C', 'CN'), 'AB', 'XX'),),
            ((('B', 'BN'), 'AN', 'BN', ('A', 'AN'), 'CA', 'XX', ('C', 'CN'), 'BC', 'XX'),),
            ((('B', 'BN'), 'AN', 'BN', ('A', 'AN'), 'BC', 'XX', ('C', 'CN'), 'CA', 'XX'),),
            ((('B', 'BN'), 'BN', 'CN', ('A', 'AN'), 'CA', 'XX', ('C', 'CN'), 'AB', 'XX'),),
            ((('B', 'BN'), 'BN', 'CN', ('A', 'AN'), 'AB', 'XX', ('C', 'CN'), 'CA', 'XX'),),

            # Grupo 9: Padrões com Var04 começando com B/BN
            ((('C', 'CN'), 'AB', 'XX', ('B', 'BN'), 'CN', 'AN', ('A', 'AN'), 'BC', 'XX'),),
            ((('C', 'CN'), 'BC', 'XX', ('B', 'BN'), 'CN', 'AN', ('A', 'AN'), 'AB', 'XX'),),
            ((('C', 'CN'), 'CA', 'XX', ('B', 'BN'), 'AN', 'BN', ('A', 'AN'), 'BC', 'XX'),),
            ((('C', 'CN'), 'BC', 'XX', ('B', 'BN'), 'AN', 'BN', ('A', 'AN'), 'CA', 'XX'),),
            ((('C', 'CN'), 'CA', 'XX', ('B', 'BN'), 'BN', 'CN', ('A', 'AN'), 'AB', 'XX'),),
            ((('C', 'CN'), 'AB', 'XX', ('B', 'BN'), 'BN', 'CN', ('A', 'AN'), 'CA', 'XX'),),

            # Grupo 10: Padrões com Var04 = B/BN e Var01 = A/AN
            ((('A', 'AN'), 'AB', 'XX', ('B', 'BN'), 'CN', 'AN', ('C', 'CN'), 'BC', 'XX'),),
            ((('A', 'AN'), 'BC', 'XX', ('B', 'BN'), 'CN', 'AN', ('C', 'CN'), 'AB', 'XX'),),
            ((('A', 'AN'), 'CA', 'XX', ('B', 'BN'), 'AN', 'BN', ('C', 'CN'), 'BC', 'XX'),),
            ((('A', 'AN'), 'BC', 'XX', ('B', 'BN'), 'AN', 'BN', ('C', 'CN'), 'CA', 'XX'),),
            ((('A', 'AN'), 'CA', 'XX', ('B', 'BN'), 'BN', 'CN', ('C', 'CN'), 'AB', 'XX'),),
            ((('A', 'AN'), 'AB', 'XX', ('B', 'BN'), 'BN', 'CN', ('C', 'CN'), 'CA', 'XX'),),

            # Grupo 11: Padrões com Var07 começando com B/BN
            ((('A', 'AN'), 'BC', 'XX', ('C', 'CN'), 'AB', 'XX', ('B', 'BN'), 'CN', 'AN'),),
            ((('A', 'AN'), 'AB', 'XX', ('C', 'CN'), 'BC', 'XX', ('B', 'BN'), 'CN', 'AN'),),
            ((('A', 'AN'), 'BC', 'XX', ('C', 'CN'), 'CA', 'XX', ('B', 'BN'), 'AN', 'BN'),),
            ((('A', 'AN'), 'CA', 'XX', ('C', 'CN'), 'BC', 'XX', ('B', 'BN'), 'AN', 'BN'),),
            ((('A', 'AN'), 'AB', 'XX', ('C', 'CN'), 'CA', 'XX', ('B', 'BN'), 'BN', 'CN'),),
            ((('A', 'AN'), 'CA', 'XX', ('C', 'CN'), 'AB', 'XX', ('B', 'BN'), 'BN', 'CN'),),

            # Grupo 12: Padrões com Var07 = B/BN e Var04 = A/AN
            ((('C', 'CN'), 'BC', 'XX', ('A', 'AN'), 'AB', 'XX', ('B', 'BN'), 'CN', 'AN'),),
            ((('C', 'CN'), 'AB', 'XX', ('A', 'AN'), 'BC', 'XX', ('B', 'BN'), 'CN', 'AN'),),
            ((('C', 'CN'), 'BC', 'XX', ('A', 'AN'), 'CA', 'XX', ('B', 'BN'), 'AN', 'BN'),),
            ((('C', 'CN'), 'CA', 'XX', ('A', 'AN'), 'BC', 'XX', ('B', 'BN'), 'AN', 'BN'),),
            ((('C', 'CN'), 'AB', 'XX', ('A', 'AN'), 'CA', 'XX', ('B', 'BN'), 'BN', 'CN'),),
            ((('C', 'CN'), 'CA', 'XX', ('A', 'AN'), 'AB', 'XX', ('B', 'BN'), 'BN', 'CN'),),

            # Grupo 13: Padrões com Var01 começando com C/CN
            ((('C', 'CN'), 'CN', 'AN', ('B', 'BN'), 'AB', 'XX', ('A', 'AN'), 'BC', 'XX'),),
            ((('C', 'CN'), 'CN', 'AN', ('B', 'BN'), 'BC', 'XX', ('A', 'AN'), 'AB', 'XX'),),
            ((('C', 'CN'), 'AN', 'BN', ('B', 'BN'), 'CA', 'XX', ('A', 'AN'), 'BC', 'XX'),),
            ((('C', 'CN'), 'AN', 'BN', ('B', 'BN'), 'BC', 'XX', ('A', 'AN'), 'CA', 'XX'),),
            ((('C', 'CN'), 'BN', 'CN', ('B', 'BN'), 'CA', 'XX', ('A', 'AN'), 'AB', 'XX'),),
            ((('C', 'CN'), 'BN', 'CN', ('B', 'BN'), 'AB', 'XX', ('A', 'AN'), 'CA', 'XX'),),

            # Grupo 14: Padrões com Var01 = C/CN e Var04 = A/AN
            ((('C', 'CN'), 'CN', 'AN', ('A', 'AN'), 'AB', 'XX', ('B', 'BN'), 'BC', 'XX'),),
            ((('C', 'CN'), 'CN', 'AN', ('A', 'AN'), 'BC', 'XX', ('B', 'BN'), 'AB', 'XX'),),
            ((('C', 'CN'), 'AN', 'BN', ('A', 'AN'), 'CA', 'XX', ('B', 'BN'), 'BC', 'XX'),),
            ((('C', 'CN'), 'AN', 'BN', ('A', 'AN'), 'BC', 'XX', ('B', 'BN'), 'CA', 'XX'),),
            ((('C', 'CN'), 'BN', 'CN', ('A', 'AN'), 'CA', 'XX', ('B', 'BN'), 'AB', 'XX'),),
            ((('C', 'CN'), 'BN', 'CN', ('A', 'AN'), 'AB', 'XX', ('B', 'BN'), 'CA', 'XX'),),

            # Grupo 15: Padrões com Var04 começando com C/CN
            ((('B', 'BN'), 'AB', 'XX', ('C', 'CN'), 'CN', 'AN', ('A', 'AN'), 'BC', 'XX'),),
            ((('B', 'BN'), 'BC', 'XX', ('C', 'CN'), 'CN', 'AN', ('A', 'AN'), 'AB', 'XX'),),
            ((('B', 'BN'), 'CA', 'XX', ('C', 'CN'), 'AN', 'BN', ('A', 'AN'), 'BC', 'XX'),),
            ((('B', 'BN'), 'BC', 'XX', ('C', 'CN'), 'AN', 'BN', ('A', 'AN'), 'CA', 'XX'),),
            ((('B', 'BN'), 'CA', 'XX', ('C', 'CN'), 'BN', 'CN', ('A', 'AN'), 'AB', 'XX'),),
            ((('B', 'BN'), 'AB', 'XX', ('C', 'CN'), 'BN', 'CN', ('A', 'AN'), 'CA', 'XX'),),

            # Grupo 16: Padrões com Var04 = C/CN e Var01 = A/AN
            ((('A', 'AN'), 'AB', 'XX', ('C', 'CN'), 'CN', 'AN', ('B', 'BN'), 'BC', 'XX'),),
            ((('A', 'AN'), 'BC', 'XX', ('C', 'CN'), 'CN', 'AN', ('B', 'BN'), 'AB', 'XX'),),
            ((('A', 'AN'), 'CA', 'XX', ('C', 'CN'), 'AN', 'BN', ('B', 'BN'), 'BC', 'XX'),),
            ((('A', 'AN'), 'BC', 'XX', ('C', 'CN'), 'AN', 'BN', ('B', 'BN'), 'CA', 'XX'),),
            ((('A', 'AN'), 'CA', 'XX', ('C', 'CN'), 'BN', 'CN', ('B', 'BN'), 'AB', 'XX'),),
            ((('A', 'AN'), 'AB', 'XX', ('C', 'CN'), 'BN', 'CN', ('B', 'BN'), 'CA', 'XX'),),

            # Grupo 17: Padrões com Var07 começando com C/CN
            ((('A', 'AN'), 'BC', 'XX', ('B', 'BN'), 'AB', 'XX', ('C', 'CN'), 'CN', 'AN'),),
            ((('A', 'AN'), 'AB', 'XX', ('B', 'BN'), 'BC', 'XX', ('C', 'CN'), 'CN', 'AN'),),
            ((('A', 'AN'), 'BC', 'XX', ('B', 'BN'), 'CA', 'XX', ('C', 'CN'), 'AN', 'BN'),),
            ((('A', 'AN'), 'CA', 'XX', ('B', 'BN'), 'BC', 'XX', ('C', 'CN'), 'AN', 'BN'),),
            ((('A', 'AN'), 'AB', 'XX', ('B', 'BN'), 'CA', 'XX', ('C', 'CN'), 'BN', 'CN'),),
            ((('A', 'AN'), 'CA', 'XX', ('B', 'BN'), 'AB', 'XX', ('C', 'CN'), 'BN', 'CN'),),

            # Grupo 18: Padrões com Var07 = C/CN e Var04 = A/AN
            ((('B', 'BN'), 'BC', 'XX', ('A', 'AN'), 'AB', 'XX', ('C', 'CN'), 'CN', 'AN'),),
            ((('B', 'BN'), 'AB', 'XX', ('A', 'AN'), 'BC', 'XX', ('C', 'CN'), 'CN', 'AN'),),
            ((('B', 'BN'), 'BC', 'XX', ('A', 'AN'), 'CA', 'XX', ('C', 'CN'), 'AN', 'BN'),),
            ((('B', 'BN'), 'CA', 'XX', ('A', 'AN'), 'BC', 'XX', ('C', 'CN'), 'AN', 'BN'),),
            ((('B', 'BN'), 'AB', 'XX', ('A', 'AN'), 'CA', 'XX', ('C', 'CN'), 'BN', 'CN'),),
            ((('B', 'BN'), 'CA', 'XX', ('A', 'AN'), 'AB', 'XX', ('C', 'CN'), 'BN', 'CN'),),

            # Grupo 19: Padrões com Var01 = AB, BC, CA (primeira parte)
            (('AB', ('A', 'AN'), 'XX', 'BC', ('B', 'BN'), 'XX', 'CA', ('C', 'CN'), 'XX'),),
            (('AB', ('A', 'AN'), 'XX', 'CA', ('B', 'BN'), 'XX', 'BC', ('C', 'CN'), 'XX'),),
            (('BC', ('A', 'AN'), 'XX', 'AB', ('B', 'BN'), 'XX', 'CA', ('C', 'CN'), 'XX'),),
            (('BC', ('A', 'AN'), 'XX', 'CA', ('B', 'BN'), 'XX', 'AB', ('C', 'CN'), 'XX'),),
            (('CA', ('A', 'AN'), 'XX', 'AB', ('B', 'BN'), 'XX', 'BC', ('C', 'CN'), 'XX'),),
            (('CA', ('A', 'AN'), 'XX', 'BC', ('B', 'BN'), 'XX', 'AB', ('C', 'CN'), 'XX'),),

            # Grupo 20: Padrões com Var01 = AB, BC, CA e Var05 = C/CN
            (('AB', ('A', 'AN'), 'XX', 'BC', ('C', 'CN'), 'XX', 'CA', ('B', 'BN'), 'XX'),),
            (('AB', ('A', 'AN'), 'XX', 'CA', ('C', 'CN'), 'XX', 'BC', ('B', 'BN'), 'XX'),),
            (('BC', ('A', 'AN'), 'XX', 'AB', ('C', 'CN'), 'XX', 'CA', ('B', 'BN'), 'XX'),),
            (('BC', ('A', 'AN'), 'XX', 'CA', ('C', 'CN'), 'XX', 'AB', ('B', 'BN'), 'XX'),),
            (('CA', ('A', 'AN'), 'XX', 'AB', ('C', 'CN'), 'XX', 'BC', ('B', 'BN'), 'XX'),),
            (('CA', ('A', 'AN'), 'XX', 'BC', ('C', 'CN'), 'XX', 'AB', ('B', 'BN'), 'XX'),),

            # Grupo 21: Padrões com Var01 = AB, BC, CA e Var02 = B/BN
            (('AB', ('B', 'BN'), 'XX', 'BC', ('A', 'AN'), 'XX', 'CA', ('C', 'CN'), 'XX'),),
            (('AB', ('B', 'BN'), 'XX', 'CA', ('A', 'AN'), 'XX', 'BC', ('C', 'CN'), 'XX'),),
            (('BC', ('B', 'BN'), 'XX', 'AB', ('A', 'AN'), 'XX', 'CA', ('C', 'CN'), 'XX'),),
            (('BC', ('B', 'BN'), 'XX', 'CA', ('A', 'AN'), 'XX', 'AB', ('C', 'CN'), 'XX'),),
            (('CA', ('B', 'BN'), 'XX', 'AB', ('A', 'AN'), 'XX', 'BC', ('C', 'CN'), 'XX'),),
            (('CA', ('B', 'BN'), 'XX', 'BC', ('A', 'AN'), 'XX', 'AB', ('C', 'CN'), 'XX'),),

            # Grupo 22: Padrões com Var01 = AB, BC, CA, Var02 = B/BN, Var05 = C/CN
            (('AB', ('B', 'BN'), 'XX', 'BC', ('C', 'CN'), 'XX', 'CA', ('A', 'AN'), 'XX'),),
            (('AB', ('B', 'BN'), 'XX', 'CA', ('C', 'CN'), 'XX', 'BC', ('A', 'AN'), 'XX'),),
            (('BC', ('B', 'BN'), 'XX', 'AB', ('C', 'CN'), 'XX', 'CA', ('A', 'AN'), 'XX'),),
            (('BC', ('B', 'BN'), 'XX', 'CA', ('C', 'CN'), 'XX', 'AB', ('A', 'AN'), 'XX'),),
            (('CA', ('B', 'BN'), 'XX', 'AB', ('C', 'CN'), 'XX', 'BC', ('A', 'AN'), 'XX'),),
            (('CA', ('B', 'BN'), 'XX', 'BC', ('C', 'CN'), 'XX', 'AB', ('A', 'AN'), 'XX'),),

            # Grupo 23: Padrões com Var01 = AB, BC, CA e Var02 = C/CN
            (('AB', ('C', 'CN'), 'XX', 'BC', ('B', 'BN'), 'XX', 'CA', ('A', 'AN'), 'XX'),),
            (('AB', ('C', 'CN'), 'XX', 'CA', ('B', 'BN'), 'XX', 'BC', ('A', 'AN'), 'XX'),),
            (('BC', ('C', 'CN'), 'XX', 'AB', ('B', 'BN'), 'XX', 'CA', ('A', 'AN'), 'XX'),),
            (('BC', ('C', 'CN'), 'XX', 'CA', ('B', 'BN'), 'XX', 'AB', ('A', 'AN'), 'XX'),),
            (('CA', ('C', 'CN'), 'XX', 'AB', ('B', 'BN'), 'XX', 'BC', ('A', 'AN'), 'XX'),),
            (('CA', ('C', 'CN'), 'XX', 'BC', ('B', 'BN'), 'XX', 'AB', ('A', 'AN'), 'XX'),),

            # Grupo 24: Padrões com Var01 = AB, BC, CA, Var02 = C/CN, Var05 = A/AN
            (('AB', ('C', 'CN'), 'XX', 'BC', ('A', 'AN'), 'XX', 'CA', ('B', 'BN'), 'XX'),),
            (('AB', ('C', 'CN'), 'XX', 'CA', ('A', 'AN'), 'XX', 'BC', ('B', 'BN'), 'XX'),),
            (('BC', ('C', 'CN'), 'XX', 'AB', ('A', 'AN'), 'XX', 'CA', ('B', 'BN'), 'XX'),),
            (('BC', ('C', 'CN'), 'XX', 'CA', ('A', 'AN'), 'XX', 'AB', ('B', 'BN'), 'XX'),),
            (('CA', ('C', 'CN'), 'XX', 'AB', ('A', 'AN'), 'XX', 'BC', ('B', 'BN'), 'XX'),),
            (('CA', ('C', 'CN'), 'XX', 'BC', ('A', 'AN'), 'XX', 'AB', ('B', 'BN'), 'XX'),),
        ]

    def _carregar_padroes_corretos_deltaaberto(self):
        """Carrega todos os padrões corretos baseados na sua lógica original"""
        return [
            # Grupo 1: Padrões com Var01 começando com A/AN/AB
            ((('A', 'AN', 'AB'), 'AN', 'BN', ('B', 'BN', 'BC'), ('BC', 'CA'), 'XX'),),
            ((('A', 'AN', 'AB'), 'AN', 'BN', ('C', 'CN', 'CA'), ('BC', 'CA'), 'XX'),),
            ((('A', 'AN', 'AB'), 'CN', 'AN', ('B', 'BN', 'BC'), ('BC', 'AB'), 'XX'),),
            ((('A', 'AN', 'AB'), 'CN', 'AN', ('C', 'CN', 'CA'), ('BC', 'AB'), 'XX'),),
            ((('A', 'AN', 'AB'), 'BN', 'CN', ('B', 'BN', 'BC'), ('CA', 'AB'), 'XX'),),
            ((('A', 'AN', 'AB'), 'BN', 'CN', ('C', 'CN', 'CA'), ('CA', 'AB'), 'XX'),),

            # Grupo 2: Padrões com Var01 começando com B/BN/BC
            ((('B', 'BN', 'BC'), 'BN', 'CN', ('C', 'CN', 'CA'), ('CA', 'AB'), 'XX'),),
            ((('B', 'BN', 'BC'), 'BN', 'CN', ('A', 'AN', 'AB'), ('CA', 'AB'), 'XX'),),
            ((('B', 'BN', 'BC'), 'AN', 'BN', ('A', 'AN', 'AB'), ('BC', 'CA'), 'XX'),),
            ((('B', 'BN', 'BC'), 'AN', 'BN', ('C', 'CN', 'CA'), ('BC', 'CA'), 'XX'),),
            ((('B', 'BN', 'BC'), 'CN', 'AN', ('A', 'AN', 'AB'), ('BC', 'AB'), 'XX'),),
            ((('B', 'BN', 'BC'), 'CN', 'AN', ('C', 'CN', 'CA'), ('BC', 'AB'), 'XX'),),

            # Grupo 3: Padrões com Var01 começando com C/CN/CA
            ((('C', 'CN', 'CA'), 'CN', 'AN', ('A', 'AN', 'AB'), ('BC', 'AB'), 'XX'),),
            ((('C', 'CN', 'CA'), 'CN', 'AN', ('B', 'BN', 'BC'), ('BC', 'AB'), 'XX'),),
            ((('C', 'CN', 'CA'), 'BN', 'CN', ('B', 'BN', 'BC'), ('CA', 'AB'), 'XX'),),
            ((('C', 'CN', 'CA'), 'BN', 'CN', ('A', 'AN', 'AB'), ('CA', 'AB'), 'XX'),),
            ((('C', 'CN', 'CA'), 'AN', 'BN', ('B', 'BN', 'BC'), ('BC', 'CA'), 'XX'),),
            ((('C', 'CN', 'CA'), 'AN', 'BN', ('A', 'AN', 'AB'), ('BC', 'CA'), 'XX'),),

            # Grupo 4: Padrões com Var04 começando com A/AN/AB
            ((('B', 'BN', 'BC'), ('BC', 'CA'), 'XX', ('A', 'AN', 'AB'), 'AN', 'BN'),),
            ((('C', 'CN', 'CA'), ('BC', 'CA'), 'XX', ('A', 'AN', 'AB'), 'AN', 'BN'),),
            ((('B', 'BN', 'BC'), ('BC', 'AB'), 'XX', ('A', 'AN', 'AB'), 'CN', 'AN'),),
            ((('C', 'CN', 'CA'), ('BC', 'AB'), 'XX', ('A', 'AN', 'AB'), 'CN', 'AN'),),
            ((('B', 'BN', 'BC'), ('CA', 'AB'), 'XX', ('A', 'AN', 'AB'), 'BN', 'CN'),),
            ((('C', 'CN', 'CA'), ('CA', 'AB'), 'XX', ('A', 'AN', 'AB'), 'BN', 'CN'),),

            # Grupo 5: Padrões com Var04 começando com B/BN/BC
            ((('C', 'CN', 'CA'), ('CA', 'AB'), 'XX', ('B', 'BN', 'BC'), 'BN', 'CN'),),
            ((('A', 'AN', 'AB'), ('CA', 'AB'), 'XX', ('B', 'BN', 'BC'), 'BN', 'CN'),),
            ((('A', 'AN', 'AB'), ('BC', 'AB'), 'XX', ('B', 'BN', 'BC'), 'CN', 'AN'),),
            ((('C', 'CN', 'CA'), ('BC', 'AB'), 'XX', ('B', 'BN', 'BC'), 'CN', 'AN'),),
            ((('A', 'AN', 'AB'), ('BC', 'CA'), 'XX', ('B', 'BN', 'BC'), 'AN', 'BN'),),
            ((('C', 'CN', 'CA'), ('BC', 'CA'), 'XX', ('B', 'BN', 'BC'), 'AN', 'BN'),),

            # Grupo 6: Padrões com Var04 começando com C/CN/CA
            ((('A', 'AN', 'AB'), ('BC', 'AB'), 'XX', ('C', 'CN', 'CA'), 'CN', 'AN'),),
            ((('B', 'BN', 'BC'), ('BC', 'AB'), 'XX', ('C', 'CN', 'CA'), 'CN', 'AN'),),
            ((('B', 'BN', 'BC'), ('CA', 'AB'), 'XX', ('C', 'CN', 'CA'), 'BN', 'CN'),),
            ((('A', 'AN', 'AB'), ('CA', 'AB'), 'XX', ('C', 'CN', 'CA'), 'BN', 'CN'),),
            ((('B', 'BN', 'BC'), ('BC', 'CA'), 'XX', ('C', 'CN', 'CA'), 'AN', 'BN'),),
            ((('A', 'AN', 'AB'), ('BC', 'CA'), 'XX', ('C', 'CN', 'CA'), 'AN', 'BN'),),

            # Grupo 7: Padrões com fases simples (primeira parte)
            ((('A', 'AN', 'AB'), ('A', 'AN'), 'XX', ('B', 'BN', 'BC'), ('B', 'BN'), 'XX'),),
            ((('A', 'AN', 'AB'), ('A', 'AN'), 'XX', ('C', 'CN', 'CA'), ('B', 'BN'), 'XX'),),
            ((('A', 'AN', 'AB'), ('A', 'AN'), 'XX', ('B', 'BN', 'BC'), ('C', 'CN'), 'XX'),),
            ((('A', 'AN', 'AB'), ('A', 'AN'), 'XX', ('C', 'CN', 'CA'), ('C', 'CN'), 'XX'),),
            ((('A', 'AN', 'AB'), ('C', 'CN'), 'XX', ('B', 'BN', 'BC'), ('A', 'AN'), 'XX'),),
            ((('A', 'AN', 'AB'), ('C', 'CN'), 'XX', ('C', 'CN', 'CA'), ('A', 'AN'), 'XX'),),
            ((('A', 'AN', 'AB'), ('C', 'CN'), 'XX', ('B', 'BN', 'BC'), ('B', 'CN'), 'XX'),),
            ((('A', 'AN', 'AB'), ('C', 'CN'), 'XX', ('C', 'CN', 'CA'), ('B', 'CN'), 'XX'),),
            ((('A', 'AN', 'AB'), ('B', 'BN'), 'XX', ('B', 'BN', 'BC'), ('A', 'AN'), 'XX'),),
            ((('A', 'AN', 'AB'), ('B', 'BN'), 'XX', ('C', 'CN', 'CA'), ('A', 'AN'), 'XX'),),
            ((('A', 'AN', 'AB'), ('B', 'BN'), 'XX', ('B', 'BN', 'BC'), ('C', 'CN'), 'XX'),),
            ((('A', 'AN', 'AB'), ('B', 'BN'), 'XX', ('C', 'CN', 'CA'), ('C', 'CN'), 'XX'),),

            # Grupo 8: Padrões com Var01 começando com B/BN/BC (fases simples)
            ((('B', 'BN', 'BC'), ('A', 'AN'), 'XX', ('A', 'AN', 'AB'), ('B', 'BN'), 'XX'),),
            ((('B', 'BN', 'BC'), ('A', 'AN'), 'XX', ('C', 'CN', 'CA'), ('B', 'BN'), 'XX'),),
            ((('B', 'BN', 'BC'), ('A', 'AN'), 'XX', ('A', 'AN', 'AB'), ('C', 'CN'), 'XX'),),
            ((('B', 'BN', 'BC'), ('A', 'AN'), 'XX', ('C', 'CN', 'CA'), ('C', 'CN'), 'XX'),),
            ((('B', 'BN', 'BC'), ('C', 'CN'), 'XX', ('A', 'AN', 'AB'), ('A', 'AN'), 'XX'),),
            ((('B', 'BN', 'BC'), ('C', 'CN'), 'XX', ('C', 'CN', 'CA'), ('A', 'AN'), 'XX'),),
            ((('B', 'BN', 'BC'), ('C', 'CN'), 'XX', ('A', 'AN', 'AB'), ('B', 'CN'), 'XX'),),
            ((('B', 'BN', 'BC'), ('C', 'CN'), 'XX', ('C', 'CN', 'CA'), ('B', 'CN'), 'XX'),),
            ((('B', 'BN', 'BC'), ('B', 'BN'), 'XX', ('A', 'AN', 'AB'), ('A', 'AN'), 'XX'),),
            ((('B', 'BN', 'BC'), ('B', 'BN'), 'XX', ('C', 'CN', 'CA'), ('A', 'AN'), 'XX'),),
            ((('B', 'BN', 'BC'), ('B', 'BN'), 'XX', ('A', 'AN', 'AB'), ('C', 'CN'), 'XX'),),
            ((('B', 'BN', 'BC'), ('B', 'BN'), 'XX', ('C', 'CN', 'CA'), ('C', 'CN'), 'XX'),),

            # Grupo 9: Padrões com Var01 começando com C/CN/CA (fases simples)
            ((('C', 'CN', 'CA'), ('A', 'AN'), 'XX', ('A', 'AN', 'AB'), ('B', 'BN'), 'XX'),),
            ((('C', 'CN', 'CA'), ('A', 'AN'), 'XX', ('B', 'BN', 'BC'), ('B', 'BN'), 'XX'),),
            ((('C', 'CN', 'CA'), ('A', 'AN'), 'XX', ('A', 'AN', 'AB'), ('C', 'CN'), 'XX'),),
            ((('C', 'CN', 'CA'), ('A', 'AN'), 'XX', ('B', 'BN', 'BC'), ('C', 'CN'), 'XX'),),
            ((('C', 'CN', 'CA'), ('C', 'CN'), 'XX', ('A', 'AN', 'AB'), ('A', 'AN'), 'XX'),),
            ((('C', 'CN', 'CA'), ('C', 'CN'), 'XX', ('B', 'BN', 'BC'), ('A', 'AN'), 'XX'),),
            ((('C', 'CN', 'CA'), ('C', 'CN'), 'XX', ('A', 'AN', 'AB'), ('B', 'CN'), 'XX'),),
            ((('C', 'CN', 'CA'), ('C', 'CN'), 'XX', ('B', 'BN', 'BC'), ('B', 'CN'), 'XX'),),
            ((('C', 'CN', 'CA'), ('B', 'BN'), 'XX', ('A', 'AN', 'AB'), ('A', 'AN'), 'XX'),),
            ((('C', 'CN', 'CA'), ('B', 'BN'), 'XX', ('B', 'BN', 'BC'), ('A', 'AN'), 'XX'),),
            ((('C', 'CN', 'CA'), ('B', 'BN'), 'XX', ('A', 'AN', 'AB'), ('C', 'CN'), 'XX'),),
            ((('C', 'CN', 'CA'), ('B', 'BN'), 'XX', ('B', 'BN', 'BC'), ('C', 'CN'), 'XX'),),
        ]

    def _verificar_padrao_individual(self, variaveis, padrao):
        """Verifica um padrão individual"""
        for i in range(len(variaveis)):
            var_atual = variaveis[i]
            padrao_atual = padrao[i]

            if isinstance(padrao_atual, tuple):
                if var_atual not in padrao_atual:
                    return False
            else:
                if var_atual != padrao_atual:
                    return False
        return True

    def eh_correto(self, lista_var):
        """Verifica se o faseamento está correto"""
        variaveis = lista_var

        if self.tip_lig == 'DF':
            padroes = self.padroes_corretos_df
        else:
            padroes = self.padroes_corretos_da

        for padrao in padroes:
            if self._verificar_padrao_individual(variaveis, padrao[0]):
                return True
        return False

    def eh_incorreto(self, *args):
        """Verifica se o faseamento está incorreto"""
        return not self.eh_correto(*args)

class ValidadorBDGD:

    def __init__(self,df,tables,output_folder: Union[str, pathlib.Path],feeders:Optional[str] = "",path_coords:Optional[str] = None):
        self.df = df
        self.feeders = feeders
        self.cod_base: str
        self.isolados: list = []
        self.voltage_dict: dict
        self.tables = tables
        self.output_folder = str(output_folder)
        self.path_coords = path_coords
        self._verification_failures: list[dict] = []

    def _cod_base_from_df(self) -> str:
        return (
            str(self.df["BASE"]["DIST"].tolist()[0])
            + str(self.df["BASE"]['DAT_EXT'].tolist()[0].split("/")[2]
                 + self.df["BASE"]['DAT_EXT'].tolist()[0].split("/")[1])
        )

    def _verification_log_path(self) -> str:
        return _verification_log_path(self.output_folder, self.feeders, self.cod_base)

    def _record_verification_failure(
        self,
        log_path: str,
        phase: str,
        step: str,
        desc: str,
        exc: BaseException,
    ) -> None:
        _log_verification_failure(log_path, phase, step, desc, exc)
        self._verification_failures.append({
            "phase": phase,
            "step": step,
            "desc": desc,
            "exc": str(exc),
        })

    def _run_step_safe(
        self,
        log_path: str,
        phase: str,
        label: str,
        desc: str,
        fn: Callable[[], object],
    ) -> Optional[pd.DataFrame]:
        _report_verification(label, desc)
        try:
            result = fn()
            if isinstance(result, pd.DataFrame):
                return result
            return None
        except Exception as exc:
            self._record_verification_failure(log_path, phase, label, desc, exc)
            _report_verification(label, f"FALHA: {exc}")
            return None

    def run_validation(self,feeder:Optional[str]=""):
        """Começa a rodar a validação da BDGD escolhida. Simula a Etapa 17 do proggeoperdas"""
        self.cod_base = self._cod_base_from_df()
        log_path = self._verification_log_path()

        df_trafo = None
        df_reg = None
        try:
            df_trafo = inner_entities_tables(self.df['EQTRMT'], self.df['UNTRMT'],
                                    left_column='UNI_TR_MT', right_column='COD_ID')
            df_reg = inner_entities_tables(self.df['EQRE'], self.df['UNREMT'],
                                    left_column='UN_RE', right_column='COD_ID')
        except Exception as exc:
            self._record_verification_failure(
                log_path, "etapa17", "prep",
                "Preparacao de transformadores e reguladores", exc,
            )
            _report_verification("prep", f"FALHA: {exc}")

        df_isolados = None
        df_total = None
        df_linhas = None
        results: list[pd.DataFrame] = []

        if df_trafo is not None and df_reg is not None:
            def run_step(label: str, desc: str, fn: Callable[[], object]) -> None:
                result = self._run_step_safe(log_path, "etapa17", label, desc, fn)
                if isinstance(result, pd.DataFrame):
                    results.append(result)

            run_step("1/31", "Energia dos alimentadores (CTMT)", lambda: ValidadorBDGD.check_ctmt_energy(self))
            run_step("2/31", "Pontos de acoplamento nulos", lambda: ValidadorBDGD.check_pacs(self))
            run_step("3/31", "Alimentadores (CTMT)", lambda: ValidadorBDGD.check_ctmt(self))
            run_step("4/31", "Linhas de media tensao (SSDMT)", lambda: ValidadorBDGD.check_lines(self, line_type='SSDMT'))
            run_step("5/31", "Linhas de baixa tensao (SSDBT)", lambda: ValidadorBDGD.check_lines(self, line_type='SSDBT'))
            run_step("6/31", "Ramais de ligacao (RAMLIG)", lambda: ValidadorBDGD.check_lines(self, line_type='RAMLIG'))
            run_step("7/31", "Chaves seccionadoras de média tensão (UNSEMT)", lambda: ValidadorBDGD.check_unse(self, chave='UNSEMT'))
            run_step("8/31", "Transformadores", lambda: ValidadorBDGD.check_transformer(self, df_trafo=df_trafo))
            run_step("9/31", "Reguladores de tensao", lambda: ValidadorBDGD.check_regulator(self, df_reg=df_reg))
            run_step("10/31", "Unidades consumidoras MT (UCMT)", lambda: ValidadorBDGD.check_ucmt(self))
            run_step("11/31", "Energia UCMT", lambda: ValidadorBDGD.check_energy(self, 'UCMT'))
            run_step("12/31", "Unidades consumidoras BT (UCBT)", lambda: ValidadorBDGD.check_loadbt(self, load_type='UCBT'))
            run_step("13/31", "Energia UCBT", lambda: ValidadorBDGD.check_energy(self, 'UCBT'))
            run_step("14/31", "Pontos de iluminacao publica (PIP)", lambda: ValidadorBDGD.check_loadbt(self, load_type='PIP'))

            def step_elem_isolados():
                nonlocal df_isolados
                dfe14, df_isolados = ValidadorBDGD.elem_isolados(self, self.df)
                return dfe14
            run_step("15/31", "Elementos isolados", step_elem_isolados)

            def step_check_feeder():
                nonlocal df_total, df_linhas
                dfe15, df_total, df_linhas = ValidadorBDGD.check_feeder(self, self.df, df_isolados, df_trafo, df_reg)
                return dfe15
            run_step("16/31", "Elementos com alimentador (CTMT) declarado errado", step_check_feeder)

            run_step("17/31", "Faseamento", lambda: ValidadorBDGD.check_faseamento(self, dataframe=self.df, lista_isolados=self.isolados))
            run_step("18/31", "Propagacao de faseamento", lambda: ValidadorBDGD.check_propagacao(self, dataframe=self.df, lista_isolados=self.isolados))
            run_step("19/31", "Tensao nos nós", lambda: ValidadorBDGD.check_voltage(self, df_total))
            run_step("20/31", "Elementos associados ao transformador(UNI_TR_MT) errado", lambda: ValidadorBDGD.iso_trafo(self, self.df, self.isolados))
            run_step("21/31", "Faseamento inconsistente com o campo MRT declarado", lambda: ValidadorBDGD.check_mrt(self, df_trafo))
            run_step("22/31", "Faseamento inconsistente com o tipo de conexão do transformador", lambda: ValidadorBDGD.phase_error(self, df_trafo, 'transformer'))
            run_step("23/31", "Bancos de transformadores inconsistentes com o seu tipo", lambda: ValidadorBDGD.bancos_trafos(self, df_trafo))
            run_step("24/31", "Faseamento DA problematico (trafo)", lambda: ValidadorBDGD.fase_df_da_problematico(self, df_trafo))
            run_step("25/31", "Tensao nos transformadores", lambda: ValidadorBDGD.check_voltage_trafo(self, df_trafo=df_trafo))
            run_step("26/31", "Faseamento inconsistente com o tipo de conexão do regulador", lambda: ValidadorBDGD.phase_error(self, df_reg, 'regcontrol'))
            run_step("27/31", "Bancos de reguladores inconsistentes com o seu tipo", lambda: ValidadorBDGD.bancos_regul(self, df_reg))
            run_step("28/31", "Faseamento DA problematico (regulador)", lambda: ValidadorBDGD.fase_df_da_problematico_regul(self, df_reg))
            run_step("29/31", "PACs iguais em linhas", lambda: ValidadorBDGD.pac_iguais(self, df_linhas))
            run_step("30/31", "Linhas em paralelo com transformadores", lambda: ValidadorBDGD.check_parallel(self, df_linhas, self.df['UNTRMT']))

        df_erros = pd.concat(results, ignore_index=True) if results else pd.DataFrame()

        _report_verification("31/31", "Exportando relatorio Excel")
        try:
            ValidadorBDGD.exportar_erros_excel(self, df_erros, self.output_folder, feeder=self.feeders)
        except Exception as exc:
            self._record_verification_failure(
                log_path, "etapa17", "31/31", "Exportando relatorio Excel", exc,
            )
            _report_verification("31/31", f"FALHA na exportacao: {exc}")

        if self._verification_failures:
            _report_verification(
                "concluido",
                f"Relatorio salvo em {self.output_folder} "
                f"({len(self._verification_failures)} etapa(s) com falha - ver {log_path})",
            )
        else:
            _report_verification("concluido", f"Relatorio salvo em {self.output_folder}")


    def elem_isolados(self,dataframe: Optional[gpd.geodataframe.GeoDataFrame] = None): #cria uma lista de elementos isolados
        """Encontra todos os elementos isolados na BDGD por alimentador (CTMT).
        dataframe = dataframe total da BDGD"""

        erros = []
        df = dataframe['CTMT'][['COD_ID','PAC_INI']]

        df_isolados = pd.DataFrame()

        feeders = df['COD_ID'].tolist()
        feeder_iterator = tqdm(feeders, desc="Isolados por alimentador", unit=" alimentador", ncols=100)
        for feeder in feeder_iterator:
            feeder_iterator.set_description(f"Isolados: {feeder}")
            alimentador = feeder
            df_trafo = inner_entities_tables(dataframe['EQTRMT'], dataframe['UNTRMT'].query("CTMT==@alimentador"),
                                    left_column='UNI_TR_MT', right_column='COD_ID')
            df_reg = inner_entities_tables(dataframe['EQRE'], dataframe['UNREMT'].query("CTMT==@alimentador"),
                                    left_column='UN_RE', right_column='COD_ID')

            adapt_regulators_names(df_trafo,'transformer')
            adapt_regulators_names(df_reg,'regulator')
            df_aux_ssdmt = assign_elem(dataframe['SSDMT'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']], 'SSDMT')
            df_aux_ssdbt = assign_elem(dataframe['SSDBT'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']], 'SSDBT')
            df_aux_ramalig = assign_elem(dataframe['RAMLIG'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']], 'RAMLIG')
            df_aux_unsemt = assign_elem(dataframe['UNSEMT'].query("CTMT == @alimentador & P_N_OPE == 'F'")[['COD_ID','CTMT','PAC_1','PAC_2']], 'UNSEMT')
            df_aux_unsebt = assign_elem(dataframe['UNSEBT'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']], 'UNSEBT')
            df_aux_trafo = assign_elem(df_trafo[['COD_ID','CTMT','PAC_1','PAC_2']], 'UNTRMT')
            df_aux_regul = assign_elem(df_reg[['COD_ID','CTMT','PAC_1','PAC_2']], 'UNREMT')
            df_ucmt = dataframe['UCMT'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC']]
            df_aux_ucmt = assign_single_pac_network(
                pd.DataFrame(df_ucmt).groupby('COD_ID', as_index=False).agg({'COD_ID':'last','CTMT':'last','PAC':'last'}),
                'UCMT',
            )
            df_ucbt = dataframe['UCBT'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC']]
            df_aux_ucbt = assign_single_pac_network(
                pd.DataFrame(df_ucbt).groupby('COD_ID', as_index=False).agg({'COD_ID':'last','CTMT':'last','PAC':'last'}),
                'UCBT',
            )
            df_aux_pip = assign_single_pac_network(dataframe['PIP'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC']], 'PIP')
            df_aux_ugbt = assign_single_pac_network(dataframe['UGBT'].query("CTMT == @alimentador")[['CEG_GD','CTMT','PAC']], 'UGBT', cod_col='CEG_GD')
            df_aux_ugmt = assign_single_pac_network(dataframe['UGMT'].query("CTMT == @alimentador")[['CEG_GD','CTMT','PAC']], 'UGMT', cod_col='CEG_GD')
            df_total = pd.concat([df_aux_ssdmt,df_aux_ssdbt,df_aux_ramalig,df_aux_unsemt,df_aux_unsebt,df_aux_trafo,df_aux_regul,
                                df_aux_pip,df_aux_ucbt,df_aux_ucmt,df_aux_ugbt,df_aux_ugmt], ignore_index=True)
            grafo = nx.Graph()
            for row in df_total.itertuples(index=False):
                try:
                    grafo.add_node(row.PAC_1)
                    grafo.add_node(row.PAC_2)
                    grafo.add_edge(row.PAC_1, row.PAC_2)
                except ValueError:
                    continue
                    print("valor do tipo none")  # TODO add in log
            try:
                grafo.remove_node('')
            except:
                pass
            pac_ctmt = df.at[df[df['COD_ID'] == feeder].index[0],'PAC_INI']
            conectados = list(nx.connected_components(grafo))
            if any(pac_ctmt in grf for grf in conectados):
                for conection in conectados:
                    if pac_ctmt in conection:
                        df_not_connected = df_total[~df_total['PAC_1'].isin(conection) & ~df_total['PAC_2'].isin(conection)]
                        break
                    else:
                        continue
                if df_not_connected.empty:
                    # print('Não existem elementos isolados!')
                    continue
                else:
                    df_isolados = pd.concat([df_isolados,df_not_connected], ignore_index=True)
                    if df_not_connected.isnull().values.any():
                        df_not_connected = df_not_connected.fillna('Nulo')
                    if self.path_coords:
                        self.plot_isolated_circuits(df_total=df_total,pac_ctmt=pac_ctmt,df_isolated=df_not_connected,feeder=feeder,output_path=self.output_folder)

                    for elem in df_not_connected.itertuples(index=False):
                        if elem.ELEM == 'SSDMT':
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"2%", "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Segmento de média tensão isolado.",
                            "detalhamento":f"Elemento SSDMT:{elem.COD_ID} - PAC_1={elem.PAC_1}, PAC_2={elem.PAC_2}, Alimentador (CTMT)={feeder}."})
                        elif elem.ELEM == 'SSDBT':
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"0.5%", "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Segmento de baixa tensão isolado.",
                            "detalhamento": f"Elemento SSDBT:{elem.COD_ID} - PAC_1={elem.PAC_1}, PAC_2={elem.PAC_2}, Alimentador (CTMT)={feeder}."})
                        elif elem.ELEM == 'RAMLIG':
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"2%", "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Ramal de baixa tensão isolado.",
                            "detalhamento": f"Elemento RAMLIG:{elem.COD_ID} - PAC_1={elem.PAC_1}, PAC_2={elem.PAC_2}, Alimentador (CTMT)={feeder}."})
                        elif elem.ELEM == 'UNTRMT':
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"0.5%", "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Transformador isolado.",
                            "detalhamento": f"Elemento UNTRMT:{elem.COD_ID} - PAC_1={elem.PAC_1}, PAC_2={elem.PAC_2}, Alimentador (CTMT)={feeder}."})
                        elif elem.ELEM == 'UNREMT':
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"2%", "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Regulador de tensão isolado.",
                            "detalhamento": f"Elemento UNREMT:{elem.COD_ID} - PAC_1={elem.PAC_1}, PAC_2={elem.PAC_2}, Alimentador (CTMT)={feeder}."})
                        elif elem.ELEM == 'UCBT':
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"2%", "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Carga BT isolada.",
                            "detalhamento":f"Elemento UCBT:{elem.COD_ID} - PAC={elem.PAC_1}, Alimentador (CTMT)={feeder}."})
                        elif elem.ELEM == 'PIP':
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"2%", "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Carga BT de iluminação pública isolada.",
                            "detalhamento":f"Elemento PIP:{elem.COD_ID} - PAC={elem.PAC_1}, Alimentador (CTMT)={feeder}."})
                        elif elem.ELEM == 'UCMT':
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"2%", "Tabela":elem.ELEM, "Código":elem.COD_ID,"erro":f"Carga MT isolada",
                            "detalhamento":f"Elemento UCMT:{elem.COD_ID} - PAC={elem.PAC_1}, Alimentador (CTMT) = {feeder}."})
                        elif elem.ELEM == 'UNSEMT':
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"2%", "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Chave MT isolada.",
                            "detalhamento": f"Elemento UNSEMT:{elem.COD_ID} - PAC_1={elem.PAC_1}, PAC_2={elem.PAC_2}, Alimentador (CTMT)={feeder}."})
                        elif elem.ELEM == 'UNSEBT':
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"2%", "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Chave BT isolada.",
                            "detalhamento":f"Elemento UNSEBT:{elem.COD_ID} - PAC_1={elem.PAC_1}, PAC_2={elem.PAC_2}, Alimentador (CTMT)={feeder}."})
                        elif elem.ELEM == 'UGBT':
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"2%", "Tabela":elem.ELEM, "Código":elem.COD_ID,"erro":f"Geração Distribuída BT isolada.",
                            "detalhamento":f"Elemento UGBT:{elem.COD_ID} - PAC_1={elem.PAC_1}, Alimentador (CTMT)={feeder}."})
                        else:
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"2%", "Tabela":elem.ELEM, "Código":elem.COD_ID,"erro":f"Geração Distribuída MT isolada.",
                            "detalhamento":f"Elemento UGMT:{elem.COD_ID} - PAC_1={elem.PAC_1}, Alimentador (CTMT)={feeder}."})
            else:
                erros.append({"COD_BASE": self.cod_base, "Erro máx":"0%", "Tabela":"CTMT", "Código":feeder, "erro":"CTMT isolado",
                "detalhamento":f"O alimentador ({feeder}) não tem conexão com a fonte."})

        df_erros = pd.DataFrame(erros)
        if not df_isolados.empty:
            self.isolados = df_isolados['COD_ID'].tolist()

        return(df_erros,df_isolados)

    def iso_trafo(self,dataframe,lista_isolados):
        """Verifica se há elementos associados a transformadores errados.
        dataframe = dataframe da tabela UNTRMT.
        df_isolados = dataframe de elementos isolados."""
        erros = []
        ##cod_id do trafo = None
        for trafo in dataframe['UNTRMT']['COD_ID'].tolist():
            if _is_missing(trafo):
                # print('trafo com COD_ID = None')
                continue
            df = dataframe['UNTRMT'].query("COD_ID == @trafo")[['COD_ID','CTMT','PAC_1','PAC_2']]
            pac_trafo = df['PAC_2'].values[0]
            df_ssdbt_teste = assign_elem(dataframe['SSDBT'].query("UNI_TR_MT == @trafo")[['COD_ID','CTMT','PAC_1','PAC_2']], 'SSDBT')
            df_ramalig_teste = assign_elem(dataframe['RAMLIG'].query("UNI_TR_MT == @trafo")[['COD_ID','CTMT','PAC_1','PAC_2']], 'RAMLIG')
            df_ucbt = dataframe['UCBT'].query("UNI_TR_MT == @trafo")[['COD_ID','CTMT','PAC']]
            df_ucbt_teste = assign_single_pac_network(
                pd.DataFrame(df_ucbt).groupby('COD_ID', as_index=False).agg({'COD_ID':'last','CTMT':'last','PAC':'last'}),
                'UCBT',
            )
            df_pip_teste = assign_single_pac_network(dataframe['PIP'].query("UNI_TR_MT == @trafo")[['COD_ID','CTMT','PAC']], 'PIP')
            df_gd_teste = assign_single_pac_network(
                dataframe['UGBT'].query("UNI_TR_MT == @trafo")[['CEG_GD','CTMT','PAC']],
                'UGBT',
                cod_col='CEG_GD',
            )
            df_total = pd.concat([df_ssdbt_teste,df_ramalig_teste,df_ucbt_teste,df_pip_teste,df_gd_teste])
            if df_total.empty:
                # print(f'trafo {trafo} sem elementos conectados')
                if df_pip_teste.empty and df_ucbt_teste.empty:
                    erros.append({"COD_BASE": self.cod_base, "Erro máx":"AVISO", "Tabela":"UNTRMT", "Código":trafo, "erro": f"Transformador sem carga associada.",
                                "detalhamento":f"O transformador {trafo} não possui nenhuma carga associada, UCBT ou PIP - (UNI_TR_MT)"})
                continue
            grafo = nx.Graph()
            for row in df_total.itertuples(index=False):
                try:
                    grafo.add_node(row.PAC_1)
                    grafo.add_node(row.PAC_2)
                    grafo.add_edge(row.PAC_1, row.PAC_2)
                except ValueError:
                    continue
                    # print("valor do tipo none")
            try:
                grafo.remove_node('')
            except:
                pass
            conectados = list(nx.connected_components(grafo))
            if any(pac_trafo in grf for grf in conectados):
                for conection in conectados:
                    if pac_trafo in conection:
                        df_not_connected = df_total[~df_total['PAC_1'].isin(conection) & ~df_total['PAC_2'].isin(conection)]
                        break
                    else:
                        continue
                if not df_not_connected.empty:
                    df_tr_errado = df_not_connected[~df_not_connected['COD_ID'].isin(lista_isolados)] #A lista de isolados em TODOS OS CTMTs deve ser criada antes dessa função aqui.
                    if not df_tr_errado.empty:
                        for elem in df_tr_errado.itertuples(index=False):
                            if elem.ELEM == 'SSDBT' or elem.ELEM == 'RAMLIG':
                                erros.append({"COD_BASE": self.cod_base, "Erro máx":"0.5%", "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"O código do transformador declarado não guarda correspondência com o do transformador obtido após o sequenciamento elétrico",
                                "detalhamento":f"O código do transformador declarado ({trafo}) não guarda correspondência com o do transformador obtido após o sequenciamento elétrico. PAC_1={elem.PAC_1}, PAC_2={elem.PAC_2}. Alimentador - {elem.CTMT}."})
                            else:
                                erros.append({"COD_BASE": self.cod_base, "Erro máx":"0.5%", "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro": f"O código do transformador declarado não guarda correspondência com o do transformador obtido após o sequenciamento elétrico",
                                "detalhamento":f"O código do transformador declarado ({trafo}) não guarda correspondência com o do transformador obtido após o sequenciamento elétrico. PAC={elem.PAC_1}. Alimentador - {elem.CTMT}"})
                continue
            else:
                # print(f'Transformador {trafo} isolado no secundário.')
                continue

        return(pd.DataFrame(erros))

    def scan_bdgd(self):
        """
        Cria geodataframes a partir da BDGD carregada e faz a verificação dos tipos de dados de acordo com
        o arquivo JSON (bdgd2dss_error.json/bdgd2dss_private_error.json).
        file_name = caminho da pasta da BDGD"""
        erros = [] #registro de erros
        lista_ctmt = []
        lista_segcon = []
        lista_untrmt = []
        lista_crvcrg = []

        gdf_ = self.df
        self.cod_base = self._cod_base_from_df()
        log_path = self._verification_log_path()

        for table_name, table in self.tables.items():
            step_label = f"scan/{table_name}"
            _report_verification("scan", f"Verificando tabela {table_name}")
            try:
                try:
                    gdf = gdf_[table_name].reset_index(drop=True)
                except KeyError:
                    gdf = gdf_[f'{table_name[:-4]}']
                if gdf.empty:
                    continue
                elif table_name == 'CTMT':
                    lista_ctmt = gdf["COD_ID"].tolist()
                elif table_name == 'SEGCON':
                    lista_segcon = gdf["COD_ID"].tolist()
                elif table_name == 'CRVCRG':
                    lista_crvcrg = gdf["COD_ID"].tolist()
                elif table_name == 'UNTRMT':
                    lista_untrmt = gdf["COD_ID"].tolist()

                erros.extend(self._scan_table_columns(
                    table_name, table, gdf, lista_ctmt, lista_segcon, lista_untrmt, lista_crvcrg,
                ))
            except Exception as exc:
                self._record_verification_failure(
                    log_path, "scan", step_label, f"Verificando tabela {table_name}", exc,
                )
                _report_verification(step_label, f"FALHA: {exc}")

        df_erros = pd.DataFrame(erros)
        try:
            ValidadorBDGD.exportar_scan_excel(self, df=df_erros, output_folder=self.output_folder, feeder=self.feeders)
        except Exception as exc:
            self._record_verification_failure(
                log_path, "scan", "scan/export", "Exportando relatorio Excel de scan", exc,
            )
            _report_verification("scan/export", f"FALHA na exportacao: {exc}")
        _report_verification("scan", "Pre-validacao concluida")
        return(df_erros)

    def _scan_table_columns(
        self,
        table_name: str,
        table,
        gdf: pd.DataFrame,
        lista_ctmt: list,
        lista_segcon: list,
        lista_untrmt: list,
        lista_crvcrg: list,
    ) -> list:
        erros = []
        for column in table.data_types.keys():
                if isinstance(table.data_types[column],list):
                    for index,value in enumerate(gdf[column]):
                        if _is_missing(value) or value == "" or value not in table.data_types[column]:
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"0%", "Tabela":f"{table_name}", "Código":f"{gdf.loc[index,table.columns[0]]}", "Índice": index,
                                    "erro":f"O atributo {column} possui valor não esperado:{gdf.loc[index,column]}. Tipo de valores esperados: {table.data_types[column]}"})
                        else:
                            continue

                elif table.data_types[column] == 'int':
                    for index,value in enumerate(gdf[column]):
                        if isinstance(value,int):
                            continue
                        else:
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"0%", "Tabela":f"{table_name}", "Código":f"{gdf.loc[index,table.columns[0]]}", "Índice": index,
                                    "erro":f"O atributo {column} possui valor não esperado:{gdf.loc[index,column]}. O valor esperado deve ser um número inteiro"})

                elif table.data_types[column] == 'float':
                    for index,value in enumerate(gdf[column]):
                        if isinstance(value,float):
                            continue
                        else:
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"0%", "Tabela":table_name, "Código":gdf.loc[index,table.columns[0]], "Índice": index,
                                    "erro":f"O atributo {column} possui valor não esperado:{gdf.loc[index,column]}. O valor esperado deve ser um número."})

                elif table.data_types[column] == 'string':
                    for index,value in enumerate(gdf[column]):
                        if isinstance(value,str) and value != "":
                            continue
                        else:
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"0%", "Tabela":f"{table_name}", "Código":f"{gdf.loc[index,table.columns[0]]}","Índice": index,
                                    "erro":f"O atributo {column} possui valor não esperado:{gdf.loc[index,column]}. O valor esperado deve ser uma string"})

                elif table.data_types[column] == "category":
                    if column == 'CTMT':
                        for index,value in enumerate(gdf[column]):
                            if _is_missing(value) or value not in lista_ctmt:
                                erros.append({"COD_BASE": self.cod_base, "Erro máx":'0%', "Tabela":f"{table_name}", "Código":f"{gdf.loc[index,table.columns[0]]}", "Índice": index,
                                    "erro":f"O atributo CTMT possui valor não esperado:{gdf.loc[index,column]}. Não está dentro da lista de CTMTs(alimentadores) desta BDGD"})
                    if column == 'TIP_CND':
                        for index,value in enumerate(gdf[column]):
                            if _is_missing(value) or value not in lista_segcon:
                                erros.append({"COD_BASE": self.cod_base, "Erro máx":"0%", "Tabela":f"{table_name}", "Código":f"{gdf.loc[index,table.columns[0]]}", "Índice": index,
                                    "erro":f"O atributo TIP_CND possui valor não esperado:{gdf.loc[index,column]}. Não está dentro da lista de SEGCONs(linecodes) desta BDGD"})
                    if column == 'UNI_TR_MT':
                        for index,value in enumerate(gdf[column]):
                            if _is_missing(value) or value not in lista_untrmt:
                                erros.append({"COD_BASE": self.cod_base, "Erro máx":"0%", "Tabela":f"{table_name}", "Código":f"{gdf.loc[index,table.columns[0]]}", "Índice": index,
                                    "erro":f"O atributo UNI_TR_MT possui valor não esperado:{gdf.loc[index,column]}. Não está dentro da lista de UNTRMTs(transformadores) desta BDGD"})
                    if column == 'TIP_CC':
                        for index,value in enumerate(gdf[column]):
                            if _is_missing(value) or value not in lista_crvcrg:
                                erros.append({"COD_BASE": self.cod_base, "Erro máx":"0%", "Tabela":f"{table_name}", "Código":f"{gdf.loc[index,table.columns[0]]}", "Índice": index,
                                    "erro":f"O atributo TIP_CC possui valor não esperado:{gdf.loc[index,column]}. Não está dentro da lista de CRVCRG(curvas de carga) desta BDGD"})
                    else:
                        continue
                else:
                    intervalo = eval(table.data_types[column])
                    inicio,fim = intervalo
                    lista = list(range(inicio,fim))
                    for index,value in enumerate(gdf[column]):
                        if _is_missing(value):
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"0%", "Tabela":f"{table_name}", "Código":f"{gdf.loc[index,table.columns[0]]}", "Índice": index,
                                    "erro":f"O atributo {column} possui valor fora dos limites:{gdf.loc[index,column]}. Os valores esperados devem estar dentro do intervalo: {table.data_types[column]}"})
                            continue
                        if int(value) not in lista:
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":"0%", "Tabela":f"{table_name}", "Código":f"{gdf.loc[index,table.columns[0]]}", "Índice": index,
                                    "erro":f"O atributo {column} possui valor fora dos limites:{gdf.loc[index,column]}. Os valores esperados devem estar dentro do intervalo: {table.data_types[column]}"})
                            continue
        return erros

    def check_model(self): #TODO fazer isso só futuramente
        ...
    def phase_error(self,merged_dfs,tipo):
        """Verifica se há uma inconsistência entre o faseamento dos enrolamentos do transformador/regulador e
        o seu tipo de ligação;
        No caso do transformador: (Monofásico, Monofásico a três fios, Bifásico, Trifásico, Delta Fechado ou Delta Aberto)
        No caso do regulador: (Monofásico, Delta Aberto, Trifásico e Delta Fechado)
        df1 = tabela EQTRMT (caso seja transformador), tabela EQRE (caso seja regulador).
        df2 = tabela UNTRMT (caso seja transformador), tabela UNREMT (caso seja regulador)
        tipo = define se será avaliado um transformador ou um regulador"""
        # 1 - M, 2 - MT, 3 - B, 4 - T, 5 - DF, 6 - DA
        dfs_divergentes = pd.DataFrame()
        erros = []

        if tipo == 'transformer':
            #CORRIGIR PARA AVALIAR O SECUNDÁRIO E PRIMÁRIO
            regras = {
            "M": ["AN", "BN", "CN","AX","BX","CX","AB","BC","CA","A","B","C"],
            "MT": ["A","B","C","AB","BC","CA","AN", "BN", "CN","AX","BX","CX"],
            "B": ["AB","BC","CA"],
            "T": ["ABC", "ABCN"],
            "DA": ["AN", "BN", "CN","AX","BX","CX","AB","BC","CA","A","B","C"],
            "DF": ["AN", "BN", "CN","AX","BX","CX","AB","BC","CA","A","B","C"]
            }
            tip = 'TIP_TRAFO'
            fas_s = 'LIG_FAS_S'
            fas_p = 'LIG_FAS_P'
            fas_t = 'LIG_FAS_T'
            x = 'MRT'

            for tip_eq, lig_fas_validos in regras.items():
                if tip_eq == 'DA' or tip_eq == 'DF':
                    df_filtrado = merged_dfs[
                        (merged_dfs[tip] == tip_eq) &
                        ((~merged_dfs[fas_p].isin(lig_fas_validos)) |
                        (~merged_dfs[fas_s].isin(lig_fas_validos[:6])))
                    ]
                elif tip_eq == 'MT':
                    df_filtrado = merged_dfs[
                        (merged_dfs[tip] == tip_eq) &
                        ((~merged_dfs[fas_p].isin(lig_fas_validos[:6])) |
                        (~merged_dfs[fas_s].isin(lig_fas_validos[6:])) |
                        (~merged_dfs[fas_t].isin(lig_fas_validos[6:])))
                    ]
                else:
                    df_filtrado = merged_dfs[
                        (merged_dfs[tip] == tip_eq) &
                        ((~merged_dfs[fas_p].isin(lig_fas_validos)) |
                        (~merged_dfs[fas_s].isin(lig_fas_validos[:3])) |
                        (~merged_dfs[fas_t].isin(['XX','0','NULL'])))
                    ]
                dfs_divergentes = pd.concat([dfs_divergentes,df_filtrado],ignore_index=True)

        else: # 1 - M, 2 - DA, 3 - T, 4 - DF
            regras = {
            "M": ["AN","BN","CN","AX","BX","CX","A","B","C"],
            "DA": ["AB", "BC", "CA","AN","BN","CN","A","B","C"],
            "T": ["ABC", "ABCN"],
            "DF": ["AB", "BC", "CA","AN","BN","CN","AX","BX","CX","A","B","C"]
            }
            tip = 'TIP_REGU'
            fas_s = 'LIG_FAS_S'
            fas_p = 'LIG_FAS_P'
            fas_t = 'FAS_CON'
            x = 'BANC'

            for tip_eq, lig_fas_validos in regras.items():
                df_filtrado = merged_dfs[
                    (merged_dfs[tip] == tip_eq) &
                    ((~merged_dfs[fas_p].isin(lig_fas_validos)) |
                    (~merged_dfs[fas_s].isin(lig_fas_validos)))
                ]
                dfs_divergentes = pd.concat([dfs_divergentes,df_filtrado],ignore_index=True)

        if tipo == 'transformer':
            for i,elem in dfs_divergentes.iterrows():
                erros.append({"COD_BASE": self.cod_base, "Erro máx":"0%", "Tabela":f"EQTRMT", "Código":elem['COD_ID'], "erro":"O código de faseamento (primário, secundário ou terciário) possui valor não esperado.",
                "detalhamento":f"O faseamento do transformador(LIG_FAS_P:{elem['LIG_FAS_P']}, LIG_FAS_S:{elem['LIG_FAS_S']}, LIG_FAS_T:{elem['LIG_FAS_T']}), não é compatível com o tipo de transformador declarado: {elem[tip]}."})
        else:
            for i,elem in dfs_divergentes.iterrows():
                erros.append({"COD_BASE": self.cod_base, "Erro máx":"0%", "Tabela":f"EQRE", "Código":f"{elem['COD_ID']}","erro":"O código de faseamento do primário ou secundário do Regulador possui valor não esperado.",
                "detalhamento":f"O faseamento do Regulador(LIG_FAS_P: {elem['LIG_FAS_P']}, LIG_FAS_S: {elem['LIG_FAS_S']}), não condiz com o tipo de regulador declarado: {elem[tip]}."})

        return(pd.DataFrame(erros))

    def bancos_trafos(self,dfs):
        """Verifica erros em bancos de transformadores que possam estar em quantidades erradas. Por exemplo: Um transformador
        em delta aberto formado por um banco de transformadores não pode ter 3 transformadores.
        dfs = df da união das tabelas EQTRMT e UNTRMT."""
        # 1 - M, 2 - MT, 3 - B, 4 - T, 5 - DF, 6 - DA

        erros = []
        contagem_valores = dfs['UNI_TR_MT'].value_counts().to_dict()
        for value, quantidade in contagem_valores.items():
            i = dfs[dfs['UNI_TR_MT'] == value].index[0]
            if dfs.loc[i,'BANC'] == 1 and quantidade == 1: #remove transformadores que sejam formado por um banco com mais de 6 trafos ou que sejam formados por bancos e não sejam bancos
                #file.write(f"UNTRMT,UNI_TR_MT,{value},BANC,1 \n")
                # print(f"UNTRMT,UNI_TR_MT,{value},BANC,1 \n")
                continue
            elif dfs.loc[i,'BANC'] == 0 and quantidade > 1:
                # print(f"UNTRMT,UNI_TR_MT,{value},BANC,0 \n")
                continue
            elif quantidade > 6:
                erros.append({"COD_BASE":self.cod_base,"Erro Máx":"0%","Tabela":"UNTRMT","COD_ID":value,"erro":"O código do banco de transformadores possui um valor não esperado. Valores esperados são 1, 2, 3, 4, 5, 6.",
                    "detalhamento":f"Quantidade de módulos do transformador [EQTRMTs] acima de 6 - {quantidade}. Alimentador - {dfs.loc[i,'CTMT']}"})
                continue
            elif quantidade <= 3:
                if (dfs.loc[i,'TIP_TRAFO'] != 'DF' or dfs.loc[i,'TIP_TRAFO'] != 'DA') and quantidade != 1:
                    erros.append({"COD_BASE":self.cod_base,"Erro Máx":"0%","Tabela":"UNTRMT","COD_ID":value, "erro":"Quantidade de módulos inconsistente para o tipo de transformador.",
                    "detalhamento":f"Quantidade de módulos do transformador [EQTRMTs] = {quantidade}. Tipo do transformador [TIP_TRAFO] = {dfs.loc[i,'TIP_TRAFO']}. Alimentador - {dfs.loc[i,'CTMT']}"})
                elif dfs.loc[i,'TIP_TRAFO'] == 'DF' and quantidade != 3:
                    erros.append({"COD_BASE":self.cod_base,"Erro Máx":"0%","Tabela":"UNTRMT","COD_ID":value,"erro":"Quantidade de módulos inconsistente para o tipo de transformador.",
                    "detalhamento":f"Quantidade de módulos do transformador [EQTRMTs] = {quantidade}. Tipo do transformador [TIP_TRAFO] = DF. Alimentador - {dfs.loc[i,'CTMT']}"})
                elif dfs.loc[i,'TIP_TRAFO'] == 'DA' and quantidade != 2:
                    erros.append({"COD_BASE":self.cod_base,"Erro Máx":"0%","Tabela":"UNTRMT","COD_ID":value,"erro":"Quantidade de módulos inconsistente para o tipo de transformador.",
                    "detalhamento":f"Quantidade de módulos do transformador [EQTRMTs] = {quantidade}. Tipo do transformador [TIP_TRAFO] = DA. Alimentador - {dfs.loc[i,'CTMT']}"})
        return(pd.DataFrame(erros))

    def fase_df_da_problematico(self,merged_dfs):
        """Verifica se as fases (primário e secundário) de um transformador do tipo Delta Fechado ou Delta Aberto, formado por um banco
        de transformadores estão declaradas corretamente.
        merged_dfs: df da união de EQTRMT e UNTRMT."""
        erros = []

        df_filtrado = merged_dfs[((merged_dfs['TIP_TRAFO'] == 'DF') | (merged_dfs['TIP_TRAFO'] == 'DA')) & merged_dfs['BANC'] == 1].copy()
        cols = ['LIG_FAS_P','LIG_FAS_S','LIG_FAS_T'] #TODO fzr nas outras verificações de faseamento
        mapa = {'AX':'AN','BX':'BN','CX':'CN'}
        df_filtrado = df_filtrado.assign(**{col: df_filtrado[col].replace(mapa) for col in cols})
        for row in df_filtrado.itertuples(index=False):
            df = df_filtrado[df_filtrado['COD_ID'] == row.COD_ID]
            lista = []
            for row in df.itertuples(index=False):
                lista.append(row.LIG_FAS_P)
                lista.append(row.LIG_FAS_S)
                lista.append(row.LIG_FAS_T)
            verificador = VerificadorFaseamentoTrafo(tip_lig=row.TIP_TRAFO)
            resultado = verificador.eh_incorreto(lista)
            if resultado:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"EQTRMT","Código":row.UNI_TR_MT,"erro":f"Faseamento interno do transformador inconsistente.",
                "detalhamento":f"Tipo do trafo - {row.TIP_TRAFO}. FAS_CON_P:{row.LIG_FAS_P},FAS_CON_S:{row.LIG_FAS_S},FAS_CON_T:{row.LIG_FAS_T}. Alimentador - {row.CTMT}"})

        return(pd.DataFrame(erros))

    def fase_df_da_problematico_regul(self,merged_dfs):
        """Verifica se as fases (primário e secundário) dos reguladores de tensão do tipo Delta Fechado ou Delta Aberto, formado por um banco
        de transformadores estão declaradas corretamente.
        merged_dfs = df da união de EQRE e UNREMT"""
        df_filtrado = merged_dfs[(merged_dfs['TIP_REGU'] == 'DF') | (merged_dfs['TIP_REGU'] == 'DA') & merged_dfs['BANC'] == 1]
        df_erro = df_filtrado[(df_filtrado['LIG_FAS_P'] != df_filtrado['LIG_FAS_S'])]
        erros = []

        for index,row in df_filtrado.iterrows():
            df = df_filtrado[df_filtrado['COD_ID'] == row['COD_ID']]
            valor_atual = [row['LIG_FAS_P']]
            df_x = df[df.index != index]
            df_y = df_x[(df_x['LIG_FAS_P'].isin(valor_atual))|(df_x['LIG_FAS_S'].isin(valor_atual))]
            if not df_y.empty:
                df_erro = pd.concat([df_erro,df_y],ignore_index=True)
        df_erro = df_erro.drop_duplicates(keep='first')#remove linhas duplicadas
        for index,elem in df_erro.iterrows():
            erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"EQRE","Código":elem['UN_RE'],"erro":f"Faseamento interno do regulador inconsistente.",
            "detalhamento":f"Tipo do regulador - {elem['TIP_TRAFO']}. FAS_CON_P:{elem['LIG_FAS_P']},FAS_CON_S:{elem['LIG_FAS_S']}. Alimentador - {elem['CTMT']}"})
        return(pd.DataFrame(erros))

    def bancos_regul(self,dfs):
        """Verifica erros em bancos de reguladores que possam estar em quantidades erradas. Por exemplo: Um regulador
        em delta fechado formado por um banco de transformadores não pode ter 2 transformadores.
        dfs = df da união das tabelas EQRE e UNREMT."""
        # 1 - M, 2 - DA, 3 - T, 4 - DF
        erros = []

        contagem_valores = dfs['COD_ID'].value_counts().to_dict()
        for value, quantidade in contagem_valores.items():
            i = dfs[dfs['COD_ID'] == value].index[0]
            if dfs.loc[i,'BANC'] == 1 and quantidade == 1: #remove transformadores que sejam formado por um banco com mais de 6 trafos ou que sejam formados por bancos e não sejam bancos
                # print(f"EQRE,UN_RE,{value},BANC,1 \n")
                continue
            elif dfs.loc[i,'BANC'] == 0 and quantidade > 1:
                # print(f"EQRE,UN_RE,{value},BANC,0 \n")
                continue
            elif quantidade > 6:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"EQRE","Código":value,"erro":f"O código do banco de reguladores possui um valor não esperado. Valores esperados são 1, 2, 3, 4, 5, 6.",
                            "detalhamento":f"A quantidade declarada de EQREs para a mesma UNREMT ({value}) foi de {quantidade}"})
                continue
            elif quantidade <= 3:
                if (dfs.loc[i,'TIP_REGU'] == 'M' or dfs.loc[i,'TIP_REGU'] == 'T') and quantidade != 1:
                    erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"EQRE","Código":value,"erro":f"Quantidade de módulos inconsistente para o tipo de regulador.",
                    "detalhamento":f"Quantidade de módulos [EQREs] = {quantidade}. Tipo do regulador = {dfs.loc[i,'TIP_REGU']}."})
                elif dfs.loc[i,'TIP_REGU'] == 'DF' and quantidade != 3:
                    erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"EQRE","Código":value,"erro":f"Quantidade de módulos inconsistente para o tipo de regulador.",
                    "detalhamento":f"Quantidade de módulos [EQREs] = {quantidade}. Tipo do regulador = DF."})
                elif dfs.loc[i,'TIP_REGU'] == 'DA' and quantidade != 2:
                    erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"EQRE","Código":value,"erro":f"Quantidade de módulos inconsistente para o tipo de regulador.",
                    "detalhamento":f"Quantidade de módulos [EQREs] = {quantidade}. Tipo do regulador = DA."})

    def check_faseamento(self,dataframe: Optional[gpd.geodataframe.GeoDataFrame] = None, lista_isolados:list = []): #Criar um dicionário de tensão por nó para verificar as tensões nos SSDMT,SSDBT,CHVMT,CHVBT e REGUL
        """Faz o faseamento de todos os elementos do alimentador específico para verificar se há alguma inconsistência entre o faseamento
        dos elementos (exemplo de erro: fase do elemento anterior AB e fase do elemento atual ABC)
        dataframe = geodataframe completo da BDGD"""
        voltage_dict = {}
        isolados = lista_isolados
        #mandar tambem o iso_trafos de elementos que estão nos trafos errados para jogar aqui em caso de não estarem isolados e não serem englobados no voltage_dict
        erros = []

        ctmt_feeders = list(enumerate(dataframe['CTMT']['COD_ID'].tolist()))
        feeder_iterator = tqdm(ctmt_feeders, desc="Faseamento", unit=" alimentador", ncols=100)
        for i, feeder in feeder_iterator:
            feeder_iterator.set_description(f"Faseamento: {feeder}")

            if dataframe['CTMT'].at[i,'ATIP'] == 1:
                erromax = ['0.5%',' em circuito atípico ']
            else:
                erromax = ['0%',' ']

            alimentador = feeder
            df_trafo = inner_entities_tables(dataframe['EQTRMT'], dataframe['UNTRMT'].query("CTMT==@alimentador"),
                                    left_column='UNI_TR_MT', right_column='COD_ID')
            df_regul = inner_entities_tables(dataframe['EQRE'], dataframe['UNREMT'].query("CTMT==@alimentador"),
                                                left_column='UN_RE', right_column='COD_ID')
            df_tr_mtmt = assign_elem(
                df_trafo[df_trafo['TEN_LIN_SE'] > 1][['COD_ID','CTMT','PAC_1','PAC_2','LIG_FAS_P','LIG_FAS_S','TEN_LIN_SE']],
                'EQTRMT',
            ).rename(columns={'LIG_FAS_S':'FAS_CON'})
            df_aux_trafo = (
                df_trafo[['COD_ID','CTMT','PAC_1','PAC_2','LIG_FAS_P','LIG_FAS_S','LIG_FAS_T','TEN_LIN_SE']]
                .rename(columns={'LIG_FAS_P':'FAS_CON'})
                .assign(ELEM='EQTRMT')
            )
            df_aux_ssdmt = assign_elem(dataframe['SSDMT'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2','FAS_CON']], 'SSDMT')
            df_aux_ssdbt = assign_elem(dataframe['SSDBT'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2','FAS_CON','UNI_TR_MT']], 'SSDBT')
            df_aux_ramalig = assign_elem(dataframe['RAMLIG'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2','FAS_CON','UNI_TR_MT']], 'RAMLIG')
            df_aux_unsemt = assign_elem(dataframe['UNSEMT'].query("CTMT == @alimentador & P_N_OPE == 'F'")[['COD_ID','CTMT','PAC_1','PAC_2','FAS_CON']], 'UNSEMT')
            df_aux_unsebt = assign_elem(dataframe['UNSEBT'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2','FAS_CON']], 'UNSEBT')
            df_aux_regul = assign_elem(df_regul[['COD_ID','CTMT','PAC_1','PAC_2','FAS_CON']], 'EQRE')
            df_ucmt = dataframe['UCMT'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC','FAS_CON']]
            df_aux_ucmt = (
                pd.DataFrame(df_ucmt).groupby('COD_ID', as_index=False)
                .agg({'COD_ID':'last','CTMT':'last','PAC':'last','FAS_CON':'last'})
                .rename(columns={'PAC':'PAC_1'})
                .assign(PAC_2='', ELEM='UCMT')
            )
            df_ucbt = dataframe['UCBT'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC','FAS_CON']]
            df_aux_ucbt = (
                pd.DataFrame(df_ucbt).groupby('COD_ID', as_index=False)
                .agg({'COD_ID':'last','CTMT':'last','PAC':'last','FAS_CON':'last'})
                .rename(columns={'PAC':'PAC_1'})
                .assign(PAC_2='', ELEM='UCBT')
            )
            df_aux_pip = (
                dataframe['PIP'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC','FAS_CON']]
                .rename(columns={'PAC':'PAC_1'})
                .assign(PAC_2='', ELEM='PIP')
            )

            df_elements = pd.concat([df_aux_ssdmt,df_aux_unsemt,df_aux_regul,df_tr_mtmt], ignore_index=True)#para faseamento de linhas
            df_aux_ucmt_tr = pd.concat([df_aux_ucmt,df_aux_trafo],ignore_index=True) #para faseamento de cargas e trafos
            df_elements_bt = pd.concat([df_aux_ssdbt,df_aux_ramalig],ignore_index=True) #para faseamento de linhas de bt
            df_aux_ucbt_pip = pd.concat([df_aux_ucbt,df_aux_pip],ignore_index=True) #para faseamento de cargas bt

            df = dataframe['CTMT'][['COD_ID','PAC_INI','TEN_NOM']]
            pac_ctmt = df.at[df[df['COD_ID'] == alimentador].index[0],'PAC_INI']
            base_kv = str(df.at[df[df['COD_ID'] == alimentador].index[0],'TEN_NOM'])

            grafo = nx.Graph()

            for row in df_elements.itertuples(index=False):
                grafo.add_node(row.PAC_1)
                grafo.add_node(row.PAC_2)
                grafo.add_edge(row.PAC_1, row.PAC_2)
            try:
                grafo.remove_node('')
            except:
                pass
            conectados = list(nx.connected_components(grafo))

            if any(pac_ctmt in grf for grf in conectados):
                sequencia = list(nx.bfs_edges(grafo,pac_ctmt))
            else:
                # print(f"Não é possível gerar a sequência elétrica, pois o alimentador {feeder} não tem conexão com a fonte")
                continue
            if sequencia[0][0] in df_elements['PAC_1'].tolist():
                pac1 = 'PAC_1'
                pac2 = 'PAC_2'
            else: #sequencia inversa
                pac1 = 'PAC_2'
                pac2 = 'PAC_1'
            #verificação de faseamento das linhas,chaves e reguladores de MT
            for seq in sequencia:
                if seq[0] == pac_ctmt:
                    voltage_dict[seq[0]] = ValidadorBDGD.convert_ten(base_kv) #dicionário de tensão, primeiros nós
                    voltage_dict[seq[1]] = ValidadorBDGD.convert_ten(base_kv)
                    continue
                else:
                    try:
                        i_ele = df_elements.index[(df_elements[pac1] == seq[0]) & (df_elements[pac2] == seq[1])].tolist()[0] #índice do elemento atual
                    except IndexError: #elemento que não está seguindo a mesma ordem dos outros
                        i_ele = df_elements.index[(df_elements[pac2] == seq[0]) & (df_elements[pac1] == seq[1])].tolist()[0]
                    cod_ele = df_elements.at[i_ele,'COD_ID'] #código do elemento atual
                    if df_elements.at[i_ele,'ELEM'] == 'EQTRMT':
                        fase_ele = df_elements.at[i_ele,'FAS_CON']
                        voltage_dict[seq[1]] = float(df_elements.at[i_ele,'TEN_LIN_SE']) #tensão do elemento atual (se trafo)
                    else:
                        fase_ele = df_elements.at[i_ele,'FAS_CON'] #fase do elemento atual
                        voltage_dict[seq[1]] = voltage_dict[seq[0]]
                        seqx = ValidadorBDGD.find_seq(sequencia=sequencia, index=sequencia.index(seq))
                        if not df_elements.index[(df_elements[pac1] == seqx[0]) & (df_elements[pac2] == seqx[1])].empty:
                            i = df_elements.index[(df_elements[pac1] == seqx[0]) & (df_elements[pac2] == seqx[1])][0] #índice do elemento anterior
                        else:
                            i = df_elements.index[(df_elements[pac1] == seqx[1]) & (df_elements[pac2] == seqx[0])][0] #elemento com ordem de PAC trocada
                        fase = df_elements.at[i,'FAS_CON'] #fase do elemento anterior
                        voltage_dict[seq[0]] = voltage_dict[df_elements.at[i,pac2]] #tensão da barra anterior
                        if set(fase_ele) <= set(fase+'N'): #verificar se a fase do elemento atual está contida no elemento anterior sem importar a ordem
                            continue #faseamento correto
                        else:
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":erromax[0], "Tabela":df_elements.at[i_ele,'ELEM'], "Código":cod_ele,"erro":f"Elemento com faseamento inadequado{erromax[1]}após sequenciamento elétrico.",
                            "detalhamento": f"Elemento analisado = {df_elements.at[i_ele,'ELEM']}:{cod_ele} - fase:{fase_ele}, Elemento de conexão - {df_elements.at[i,'ELEM']}:{df_elements.at[i,'COD_ID']}. Fase de conexão - {fase}. Alimentador:{df_elements.at[i_ele,'CTMT']}."})
            #verificação do faseamento nos transformadores e nas cargas de média tensão
            for elem in df_aux_ucmt_tr.itertuples(index=False):
                if elem.COD_ID in isolados: #remove os elementos isolados da checagem de faseamento
                    continue
                indices = df_elements.index[(df_elements[pac2] == elem.PAC_1)|(df_elements[pac1] == elem.PAC_1)].tolist()
                if len(indices) == 0: #elemento conectado no nível de tensão errado
                    continue
                if len(indices) == 1:
                    fase = df_elements.at[indices[0],'FAS_CON']
                    if set(elem.FAS_CON) <= set(fase+'N'):
                        continue #faseamento correto
                    else:
                        erros.append({"COD_BASE": self.cod_base, "Erro máx":erromax[0], "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Elemento com faseamento inadequado{erromax[1]}após sequenciamento elétrico.",
                        "detalhamento":f"Elemento analisado = {elem.ELEM}:{elem.COD_ID} - fase:{elem.FAS_CON}, Elemento de conexão - {df_elements.at[i,'ELEM']}:{df_elements.at[i,'COD_ID']}. Fase de conexão - {fase}. Alimentador: {elem.CTMT}."})
                else:
                    find = False
                    for i in indices:
                        fase = df_elements.at[i,'FAS_CON']
                        if set(elem.FAS_CON) <= set(fase+'N'):
                            find = True
                            break
                    if find:
                        continue
                    erros.append({"COD_BASE": self.cod_base, "Erro máx":erromax[0], "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Elemento com faseamento inadequado{erromax[1]}após sequenciamento elétrico.",
                    "detalhamento":f"Elemento analisado = {elem.ELEM}:{elem.COD_ID} - fase:{elem.FAS_CON}, Elemento de conexão - {df_elements.at[i,'ELEM']}:{df_elements.at[i,'COD_ID']}. Fase de conexão - {fase}. Alimentador: {elem.CTMT}."})
            #faseamento das linhas de baixa tensão
            for trafo in df_aux_trafo['COD_ID'].tolist():
                if _is_missing(trafo):
                    # print('trafo sem COD_ID')
                    continue
                sequencia,grafo = ValidadorBDGD.return_graph_trafo(df_aux_trafo,df_elements_bt,trafo)
                if sequencia:
                    pac_trafo = sequencia[0][0]
                    for seq in sequencia:
                        if seq[0] == pac_trafo:
                            i = df_aux_trafo.index[df_aux_trafo['COD_ID'] == trafo].tolist()[0]
                            fase = df_aux_trafo.at[i,'LIG_FAS_S']+df_aux_trafo.at[i,'LIG_FAS_T'] #fase do elemento anterior
                            voltage_dict[seq[0]] = df_aux_trafo.at[i,'TEN_LIN_SE']
                            voltage_dict[seq[1]] = voltage_dict[seq[0]]
                        else:
                            try:
                                i_ele = df_elements_bt.index[(df_elements_bt[pac1] == seq[0]) & (df_elements_bt[pac2] == seq[1])].tolist()[0] #índice do elemento atual
                            except IndexError:
                                i_ele = df_elements_bt.index[(df_elements_bt[pac1] == seq[1]) & (df_elements_bt[pac2] == seq[0])].tolist()[0]
                            cod_ele = df_elements_bt.at[i_ele,'COD_ID'] #código do elemento atual
                            fase_ele = df_elements_bt.at[i_ele,'FAS_CON'] #fase do elemento atual
                            voltage_dict[seq[1]] = voltage_dict[seq[0]]
                            seqx = ValidadorBDGD.find_seq(sequencia=sequencia, index=sequencia.index(seq)) #acha o primeiro elemento anterior no grafo
                            if not df_elements_bt.index[(df_elements_bt[pac1] == seqx[0]) & (df_elements_bt[pac2] == seqx[1])].empty:
                                i = df_elements_bt.index[(df_elements_bt[pac1] == seqx[0]) & (df_elements_bt[pac2] == seqx[1])].tolist()[0]
                            else:
                                i = df_elements_bt.index[(df_elements_bt[pac1] == seqx[1]) & (df_elements_bt[pac2] == seqx[0])].tolist()[0]
                            fase = df_elements_bt.at[i,'FAS_CON'] #fase do elemento anterior
                            voltage_dict[seq[0]] = voltage_dict[df_elements_bt.at[i,pac2]]#tensão da barra anterior
                        if set(fase_ele) <= set(fase+'N'): #verificar se a fase do elemento atual está contida no elemento anterior sem importar a ordem
                            continue #faseamento correto
                        else:
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":erromax[0], "Tabela":df_elements_bt.at[i_ele,'ELEM'], "Código":df_elements_bt.at[i_ele,'COD_ID'],"erro":f"Elemento com faseamento inadequado{erromax[1]}após sequenciamento elétrico.",
                            "detalhamento":f"Elemento analisado = {df_elements_bt.at[i_ele,'ELEM']}:{df_elements_bt.at[i_ele,'COD_ID']} - fase:{fase_ele}, Elemento de conexão = {df_elements_bt.at[i,'ELEM']}:{df_elements_bt.at[i,'COD_ID']}. Fase de conexão - {fase}. Alimentador: {df_elements_bt.at[i_ele,'CTMT']}."})
                else:
                    i = df_aux_trafo[df_aux_trafo['COD_ID'] == trafo].index[0]
                     #TODO se não há nada conectado no secundário, devo atribuir alguma tensão a esse nó???
                    voltage_dict[df_aux_trafo.at[i,'PAC_2']] = df_aux_trafo.at[i,'TEN_LIN_SE']
                    # print(f'trafo {trafo} sem linhas de baixa tensão ou cargas!')
            #faseamento das cargas de baixa tensão
            for elem in df_aux_ucbt_pip.itertuples(index=False):
                if elem.COD_ID in lista_isolados:#remove os elementos isolados da checagem de faseamento
                        continue
                #TODO ver a fase do trafo se tiver conectada diretamente no trafo
                i_tr = df_aux_trafo.index[(df_aux_trafo['PAC_2'] == elem.PAC_1)].tolist()
                if len(i_tr) > 0:
                    if set(elem.FAS_CON) <= set(str(df_aux_trafo.loc[i_tr,'LIG_FAS_S']+'N')) or set(elem.FAS_CON) <= set(str(df_aux_trafo.loc[i_tr,'LIG_FAS_T']+'N')):
                        continue #faseamento correto
                    else:
                        erros.append({"COD_BASE": self.cod_base, "Erro máx":erromax[0], "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Elemento com faseamento inadequado{erromax[1]}após sequenciamento elétrico.",
                        "detalhamento":f"Elemento analisado = {elem.ELEM}:{elem.COD_ID} - fase:{elem.FAS_CON}, Elemento de conexão - {df_elements_bt.at[i,'ELEM']}:{df_elements_bt.at[i,'COD_ID']}. Fase de conexão - {fase}. Alimentador: {elem.CTMT}."})
                indices = df_elements_bt.index[(df_elements_bt['PAC_2'] == elem.PAC_1)|(df_elements_bt['PAC_1'] == elem.PAC_1)].tolist()
                if len(indices) == 1:
                    i = indices[0]
                    fase = df_elements_bt.at[i,'FAS_CON']
                    if set(elem.FAS_CON) <= set(fase+'N'):
                        continue #faseamento correto
                    else:
                        erros.append({"COD_BASE": self.cod_base, "Erro máx":erromax[0], "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Elemento com faseamento inadequado{erromax[1]}após sequenciamento elétrico.",
                        "detalhamento":f"Elemento analisado = {elem.ELEM}:{elem.COD_ID} - fase:{elem.FAS_CON}, Elemento de conexão - {df_elements_bt.at[i,'ELEM']}:{df_elements_bt.at[i,'COD_ID']}. Fase de conexão - {fase}. Alimentador: {elem.CTMT}."})
                elif len(indices) > 1:
                    find = False
                    for i in indices:
                        fase = df_elements_bt.at[i,'FAS_CON']
                        if set(elem.FAS_CON) <= set(fase+'N'):
                            find = True
                            break
                    if find:
                        continue
                    erros.append({"COD_BASE": self.cod_base, "Erro máx":erromax[0], "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Elemento com faseamento inadequado{erromax[1]}após sequenciamento elétrico.",
                    "detalhamento":f"Elemento analisado = {elem.ELEM}:{elem.COD_ID} - fase:{elem.FAS_CON}, Elemento de conexão - {df_elements_bt.at[i,'ELEM']}:{df_elements_bt.at[i,'COD_ID']}. Fase de conexão - {fase}. Alimentador: {elem.CTMT}."})
                else:#se não tiver num ramal ou linha de BT está em um trafo
                    try:
                        i = df_aux_trafo.index[df_aux_trafo['PAC_2'] == elem.PAC_1].tolist()[0]
                    except:
                        i = df_aux_trafo.index[df_aux_trafo['PAC_1'] == elem.PAC_1].tolist()[0]
                        # print(f'Carga BT {elem.COD_ID} conectada no primário do trafo {df_aux_trafo.at[i, "COD_ID"]}!!')
                    fase = df_elements_bt.at[i,'FAS_CON']
                    if set(elem.FAS_CON) <= set(fase+'N'):
                        continue #faseamento correto
                    else:
                        erros.append({"COD_BASE": self.cod_base, "Erro máx":erromax[0], "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Elemento com faseamento inadequado{erromax[1]}após sequenciamento elétrico.",
                        "detalhamento":f"Elemento analisado = {elem.ELEM}:{elem.COD_ID} - fase:{elem.FAS_CON}, Elemento de conexão - {df_elements_bt.at[i,'ELEM']}:{df_elements_bt.at[i,'COD_ID']}. Fase de conexão - {fase}. Alimentador: {elem.CTMT}."})
            #df_total = pd.concat([df_elements,df_aux_ucmt_tr,df_elements_bt,df_aux_ucbt_pip],ignore_index=True)
        self.voltage_dict = voltage_dict
        return(pd.DataFrame(erros))

    def check_voltage(self,df):
        """Verifica se os níveis de tensão nos elementos estão corretos. Por exemplo: Linhas de média tensão com tensão nas barras abaixo de 2,3 kV são um erro
        df = dataframe completo com a coluna 'ELEM' """
        voltage_dict = self.voltage_dict
        erros = []
        for elem in df.itertuples(index=False):
            if elem.COD_ID in self.isolados:
                continue
            try: #elementos que estão isolados do seu trafo (são de outros trafos vão dar erro)
                if elem.ELEM == 'UNTRMT' or elem.ELEM == 'UCMT':
                    if voltage_dict[elem.PAC_1] < 2.3:
                        erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":elem.ELEM,"Código":elem.COD_ID,"erro":f"{elem.ELEM} com tensão inadequada após sequenciamento elétrico.",
                        "detalhamento":f"Tensão inadequada - {voltage_dict[elem.PAC_1]} kV< 2.3 kV. Barra PAC:{elem.PAC_1}. Alimentador - {elem.CTMT}"})
                elif elem.ELEM == 'UCBT' or elem.ELEM == 'PIP':
                    if voltage_dict[elem.PAC_1] >= 2.3:
                        erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":elem.ELEM,"Código":elem.COD_ID,"erro":f"{elem.ELEM} com tensão inadequada após sequenciamento elétrico.",
                        "detalhamento":f"Tensão inadequada - {voltage_dict[elem.PAC_1]} kV>2.3 kV. Barra PAC:{elem.PAC_1}. Alimentador - {elem.ELEM}."})
                elif (elem.ELEM == 'SSDBT' or elem.ELEM == 'RAMLIG' or elem.ELEM == 'UNSEBT'):
                    if voltage_dict[elem.PAC_1] >= 2.3 or voltage_dict[elem.PAC_2] >= 2.3:
                        erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":elem.ELEM,"Código":elem.COD_ID,"erro":f"{elem.ELEM} com tensão inadequada após sequenciamento elétrico.",
                        "detalhamento":f"Tensão inadequada {voltage_dict[elem.PAC_1]} kV>2.3 kV. Barras PAC_1:{elem.PAC_1}, PAC_2:{elem.PAC_2}. Alimentador - {elem.ELEM}."})
                else:
                    if voltage_dict[elem.PAC_1] < 2.3 or voltage_dict[elem.PAC_2] < 2.3:
                        erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":elem.ELEM,"Código":elem.COD_ID,"erro":f"{elem.ELEM} com tensão inadequada após sequenciamento elétrico.",
                        "detalhamento":f"Tensão inadequada {voltage_dict[elem.PAC_2]} kV<2.3 kV. Barras PAC_1:{elem.PAC_1}, PAC_2:{elem.PAC_2}. Alimentador - {elem.ELEM}."})
            except KeyError:
                continue
        return(pd.DataFrame(erros))

    def check_voltage_trafo(self,df_trafo):
        """Verifica se a tensão no primário e secundário do trafo são iguais, pois isso representa um erro
        df_trafo = dataframe com todos os trafos da BDGD"""
        voltage_dict = self.voltage_dict
        erros = []
        for trf in df_trafo.itertuples(index=False):
            if trf.COD_ID in self.isolados:
                continue
            try:
                v1 = voltage_dict[trf.PAC_1]
                v2 = voltage_dict[trf.PAC_2]
            except:
                continue
                # print(f'erro pac1 - {trf.PAC_1}/ pac2 - {trf.PAC_2}. COD_ID - {trf.COD_ID}')
            if v1 == v2:
                if self.df['CTMT'].at[self.df['CTMT'][self.df['CTMT']['COD_ID'] == trf.CTMT].index[0],'ATIP'] == 1:
                    erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"UNTRMT","Código":trf.COD_ID,"erro":"Tensão no Primário igual ao Secundário no Trafo. Possível inversão de sequencia elétrica em circuito atípico.",
                    "detalhamento":f"Tensão no primário do trafo (PAC_1:{trf.PAC_1}) igual a do secundário (PAC_2:{trf.PAC_2}). Tensão:{v1} kV. Alimentador - {trf.CTMT}"})
                else:
                    erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"UNTRMT","Código":trf.COD_ID,"erro":"Tensão no Primário igual ao Secundário no Trafo após sequenciamento elétrico.",
                    "detalhamento":f"Tensão no primário do trafo (barra:{trf.PAC_1}) igual a do secundário (barra:{trf.PAC_2}). Tensão:{v1} kV. Alimentador - {trf.CTMT}"})

    def pac_iguais(self,df):
        """ Verifica se os pontos de acoplamento elétrico são iguais para as linhas de média e baixa tensão, incluindo ramais.
        df = dataframe de todas as linhas de média e baixa tensão (incluindo ramais).
        tabela = indica qual tabela está sendo verificada."""

        erros = []
        indices = df[(df['PAC_1'] == df['PAC_2'])].index.tolist()
        if len(indices) > 0:
            for i in indices:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":df.loc[i,'ELEM'],"Código":df.loc[i,'COD_ID'],"erro":"O ponto de acoplamento 1 tem que ser diferente do ponto de acoplamento 2.",
                "detalhamento":f"PAC_1 e PAC_2 = {df.loc[i,'PAC_1']}. Alimentador - {df.loc[i,'CTMT']}"})
        else:
            return pd.DataFrame()
        return(pd.DataFrame(erros))

    def check_mrt(self,df):
        """Verifica se há inconsistência entre o tipo de transformador e seu faseamento e o que foi declarado no campo MRT da entidade
        df = dataframe da união das tabelas EQTRMT e UNTRMT"""

        erros = []
        fas_p = ['A','B','C']
        fas_s = ['AN','BN','CN']
        fas_t = fas_s + ['XX','NULL','0']

        df = df[df['MRT'] == 1]
        #TODO continuar aqui
        df_erro = df[((df['TIP_TRAFO'] != 'M') & (df['TIP_TRAFO'] != 'MT'))|(~df['LIG_FAS_P'].isin(fas_p))|(~df['LIG_FAS_S'].isin(fas_s))|((~df['LIG_FAS_T'].isin(fas_t)))]
        if not df_erro.empty:
            for elem in df_erro.itertuples(index=False):
                if not _is_missing(elem.TIP_TRAFO) and elem.TIP_TRAFO != 'M' and elem.TIP_TRAFO != 'MT':
                    erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"UNTRMT","Código":elem.COD_ID,"erro":"Tipo do transformador inconsistente com o valor declarado no campo MRT.",
                    "detalhamento":f"Tipo do transformador:{elem.TIP_TRAFO} inconsistente com o valor declarado no campo MRT (1). Tipos válidos: M, MT. Alimentador - {elem.CTMT}"})
                elif _invalid_enum(elem.LIG_FAS_P, fas_p):
                    erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"UNTRMT","Código":elem.COD_ID,"erro":"Código da fase primária não é compatível com o valor declarado no campo MRT.",
                    "detalhamento":f"Código da fase primária ({elem.LIG_FAS_P}) inconsistente com o valor do campo MRT (1). Códigos válidos: A,B,C. Alimentador - {elem.CTMT}"})
                elif _invalid_enum(elem.LIG_FAS_S, fas_s):
                    erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"UNTRMT","Código":elem.COD_ID,"erro":"Código da fase secundária não é compatível com o valor declarado no campo MRT.",
                    "detalhamento":f"Código da fase secundária ({elem.LIG_FAS_S}) inconsistente com o valor do campo MRT (1). Códigos válidos: AN,BN,CN. Alimentador - {elem.CTMT}"})
                else:
                    erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"UNTRMT","Código":elem.COD_ID,"erro":"Código da fase terciária não é compatível com o valor declarado no campo MRT.",
                    "detalhamento":f"Código da fase terciária ({elem.LIG_FAS_T}) inconsistente com o valor do campo MRT (1). Códigos válidos: AN,BN,CN,XX,NULL. Alimentador - {elem.CTMT}"})

        return(pd.DataFrame(erros))

    def check_feeder(self,df,df_isolados,df_tr,df_re):
        """verifica se alguma elemento da lista de isolados tem conexão com algum PAC do df geral com todos os alimentadores
        pois isso é sinal de que o elemento está atribuído ao alimentador errado
        df = dataframe completo da BDGD
        df_isolados = dataframe dos elementos isolados da BDGD"""


        erros = []

        df_trafo = df_tr
        df_reg = df_re

        adapt_regulators_names(df_trafo,'transformer')
        adapt_regulators_names(df_reg,'regulator')

        df_aux_ssdmt = assign_elem(df['SSDMT'][['COD_ID','CTMT','PAC_1','PAC_2']], 'SSDMT')
        df_aux_ssdbt = assign_elem(df['SSDBT'][['COD_ID','CTMT','PAC_1','PAC_2']], 'SSDBT')
        df_aux_ramalig = assign_elem(df['RAMLIG'][['COD_ID','CTMT','PAC_1','PAC_2']], 'RAMLIG')
        df_aux_unsemt = assign_elem(df['UNSEMT'].query("P_N_OPE == 'F'")[['COD_ID','CTMT','PAC_1','PAC_2']], 'UNSEMT')
        df_aux_unsebt = assign_elem(df['UNSEBT'][['COD_ID','CTMT','PAC_1','PAC_2']], 'UNSEBT')
        df_aux_trafo = assign_elem(df_trafo[['COD_ID','CTMT','PAC_1','PAC_2']], 'UNTRMT')
        df_aux_regul = assign_elem(df_reg[['COD_ID','CTMT','PAC_1','PAC_2']], 'UNREMT')
        df_aux_ucmt = assign_single_pac_network(df['UCMT'][['COD_ID','CTMT','PAC']], 'UCMT')
        df_aux_ucbt = assign_single_pac_network(df['UCBT'][['COD_ID','CTMT','PAC']], 'UCBT')
        df_aux_pip = assign_single_pac_network(df['PIP'][['COD_ID','CTMT','PAC']], 'PIP')
        df_aux_ugbt = assign_single_pac_network(df['UGBT'][['CEG_GD','CTMT','PAC']], 'UGBT', cod_col='CEG_GD')
        df_aux_ugmt = assign_single_pac_network(df['UGMT'][['CEG_GD','CTMT','PAC']], 'UGMT', cod_col='CEG_GD')

        df_total = pd.concat([df_aux_ssdmt,df_aux_ssdbt,df_aux_ramalig,df_aux_unsemt,df_aux_unsebt,df_aux_trafo,df_aux_regul,
                                df_aux_pip,df_aux_ucbt,df_aux_ucmt,df_aux_ugbt,df_aux_ugmt], ignore_index=True)
        df_total = normalize_pac_columns(df_total)

        df_linhas = pd.concat([df_aux_ssdmt,df_aux_ssdbt,df_aux_ramalig], ignore_index=True)

        if df_isolados.empty:
            return(pd.DataFrame(erros),df_total,df_linhas)

        df_isolados = normalize_pac_columns(df_isolados.copy())
        df_1 = df_total.merge(df_isolados, how='left', indicator=True)
        df_1 = df_1[df_1['_merge'] == 'left_only']
        df_1 = df_1.drop(columns=['_merge'])
        df_filtrado = df_1[df_1['CTMT'].isin(df['CTMT']['COD_ID'].tolist())].copy()

        for elem in df_isolados.itertuples(index=False):
            if elem.ELEM == 'UNREMT':
                erromax = '2%'
            else:
                erromax = '0.5%'
            ctmt_correto = lookup_ctmt_by_pac(df_filtrado, elem.PAC_1)
            if ctmt_correto is not None:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":erromax,"Tabela":elem.ELEM,"Código":elem.COD_ID,"erro":"O código do alimentador declarado não guarda correspondência com o alimentador obtido após o sequenciamento elétrico",
                "detalhamento":f"Código do alimentador declarado (CTMT) = {elem.CTMT}, Código do alimentador obtido após sequenciamento elétrico = {ctmt_correto}."})
                continue
            ctmt_correto = lookup_ctmt_by_pac(df_filtrado, elem.PAC_2)
            if ctmt_correto is not None:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":erromax,"Tabela":elem.ELEM,"Código":elem.COD_ID,"erro":"O código do alimentador declarado não guarda correspondência com o alimentador obtido após o sequenciamento elétrico",
                "detalhamento":f"Código do alimentador declarado (CTMT) = {elem.CTMT}, Código do alimentador obtido após sequenciamento elétrico = {ctmt_correto}."})
        return(pd.DataFrame(erros),df_total,df_linhas)

    def check_parallel(self,df1,df2):
        """checa se há alguma linha de MT, BT, ramal de ligação ou chave MT ou BT em paralelo com algum transformador no alimentador
        df1 = dataframe da união das tabelas SSDMT, SSDBT e RAMLIG
        df2 = dataframe da tabela UNTRMT"""
        erros = []

        df1 = df1.copy().assign(pares=df1.apply(lambda x: frozenset([x['PAC_1'], x['PAC_2']]), axis=1))
        df2 = df2.copy().assign(pares=df2.apply(lambda x: frozenset([x['PAC_1'], x['PAC_2']]), axis=1))
        df_filtrado = df1[df1['pares'].isin(df2['pares'])].drop(columns='pares')
        if not df_filtrado.empty:
            for i,line in df_filtrado.iterrows():
                trafo = df2[(df2['PAC_1'] == line['PAC_1'])&(df2['PAC_2'] == line['PAC_2'])|(df2['PAC_1'] == line['PAC_2'])&(df2['PAC_2'] == line['PAC_1'])]['COD_ID']
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":line['ELEM'],"Código":line['COD_ID'],"erro":f"O elemento declarado na tabela {line['ELEM']} está em paralelo com o transformador de distribuição declarado na tabela TrafoMTMTMTBT",
                "detalhamento":f"Elemento {line['ELEM']}:{line['COD_ID']}, barras: PAC_1 = {line['PAC_1']} e PAC_2 = {line['PAC_2']}. Transformador: {trafo}. Alimentador - {line['CTMT']}."})
            return(pd.DataFrame(erros))
        return pd.DataFrame()

    def check_pacs(self):
        """Checa se há algum ponto de acoplamento elétrico com valor nulo"""
        erros = []
        keys = [x for x in self.df.keys() if x not in ['BASE','SEGCON','CRVCRG','UNTRAT','EQTRMT','EQRE']]
        for key in keys:
            if key == 'CTMT':
                for elem in self.df[key].itertuples(index=False):
                   if _is_undeclared_string(elem.PAC_INI):
                        erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":key,"Código":elem.COD_ID,"erro":f"Não foi declarado o ponto de acoplamento inicial.","detalhamento":
                                      f"Ponto de acoplamento inicial do alimentador não foi declarado."})
            elif key in ['UCMT','UCBT','UGBT','UGMT','PIP']:
                for elem in self.df[key].itertuples(index=False):
                   if _is_undeclared_string(elem.PAC):
                        erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":key,"Código":elem.COD_ID,"erro":f"Não foi declarado o ponto de acoplamento 1.","detalhamento":
                                      f"Ponto de acoplamento declarado = {elem.PAC}. Alimentador - {elem.CTMT}."})
            else:
                for elem in self.df[key].itertuples(index=False):
                   if _is_undeclared_string(elem.PAC_1):
                        erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":key,"Código":elem.COD_ID,"erro":f"Não foi declarado o ponto de acoplamento 1.","detalhamento":
                                      f"Ponto de acoplamento declarado = {elem.PAC_1}.  Alimentador - {elem.CTMT}."})
                   elif _is_undeclared_string(elem.PAC_2):
                        erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":key,"Código":elem.COD_ID,"erro":f"Não foi declarado o ponto de acoplamento 2.","detalhamento":
                                      f"Ponto de acoplamento declarado = {elem.PAC_2}.  Alimentador - {elem.CTMT}."})
        return(pd.DataFrame(erros))

    def check_transformer(self,df_trafo):
        """Faz a verificação dos parâmetros do transformador MTMTMTBT
        df_trafo = dataframe da união do UNTRMT e EQTRMT"""
        erros = []
        fas_p = ["A","B","C","AN","BN","CN","AX","BX","CX","AB","BC","CA","ABC","ABCN"]
        fas_s = ["A","B","C","AN","BN","CN","AX","BX","CX","AB","BC","CA","ABN","BCN","CAN","ABC","ABCN"]
        fas_t = ["A","B","C","AN","BN","CN","AX","BX","CX","AB","BC","CA","ABC","ABCN","XX","0"]
        for trafo in df_trafo.itertuples(index=False):
            if (_invalid_or_missing_enum(trafo.LIG_FAS_P, fas_p)
                    or _invalid_or_missing_enum(trafo.LIG_FAS_S, fas_s)
                    or _invalid_or_missing_enum(trafo.LIG_FAS_T, fas_t)):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"EQTRMT","Código":trafo.COD_ID,"erro":f"O código do faseamento (primário, secundário ou terciário) possui um valor não esperado. Valores esperados são ABCN, ABC, AB, BC, CA, AN, BN, CN, A, B, C, e para o caso do terciário também valores nulos.",
                "detalhamento":f"Fase de primário:{trafo.LIG_FAS_P},Fase de secundário:{trafo.LIG_FAS_S},Fase de terciário:{trafo.LIG_FAS_T}. Alimentador: {trafo.CTMT}"})
            if _invalid_or_missing_enum(trafo.TIP_TRAFO, ["M","B","T","MT","DA","DF"]):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"UNTRMT","Código":trafo.COD_ID,"erro":f"O código do tipo de transformador possui um valor não esperado. Valores esperados são M, B, MT, T, DF, DA.",
                "detalhamento":f"O tipo de transformador declarado no campo 'TIP_TRAFO' foi: {trafo.TIP_TRAFO}. Alimentador: {trafo.CTMT}"})
            if _is_missing(trafo.MRT) or trafo.MRT not in [0, 1]:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"UNTRMT","Código":trafo.COD_ID,"erro":f"O campo para identificar transformador MRT possui valor não esperado. Valores esperados são 0,1.",
                "detalhamento":f"O tipo de transformador declarado no campo 'TIP_TRAFO' foi: {trafo.TIP_TRAFO} e no campo MRT: {trafo.MRT}. Alimentador: {trafo.CTMT}"})
            if _invalid_or_missing_enum(trafo.ARE_LOC, ['UB','NU']):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"UNTRMT","Código":trafo.COD_ID,"erro":f"O campo para identificar a classe do transformador possui valor não esperado. Valores esperados são 'UB', 'NU'.",
                "detalhamento":f"O campo 'ARE_LOC' foi declarado como: {trafo.ARE_LOC}. Alimentador: {trafo.CTMT}"})
            if _invalid_or_missing_enum(trafo.POS, ["PD", "OD", "T", "G", "CS", "CO", "A", "O"]):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"UNTRMT","Código":trafo.COD_ID,"erro":f"O campo para identificar o proprietário do transformador possui valor não esperado. Valores esperados são 'PD', 'OD', 'T', 'G', 'CS', 'CO', 'A', 'O'.",
                "detalhamento":f"O campo 'POS' foi declarado como: {trafo.POS}. Alimentador: {trafo.CTMT}"})
            if trafo.PER_FER > trafo.PER_TOT:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"UNTRMT","Código":trafo.COD_ID,"erro":f"O campo da perda potência ferro é maior que o campo perda potência total. Valores esperados são abaixo da perda potência total.",
                "detalhamento":f"O valor de perda total do trafo ('PER_TOT') é de: {trafo.PER_TOT}, sendo menor que as perdas no ferro (PER_FER), de :{trafo.PER_FER}. Alimentador: {trafo.CTMT}"})
            if (not _is_missing(trafo.LIG_FAS_T) and trafo.LIG_FAS_T not in ['AN','BN','CN','XX']
                    and trafo.LIG_FAS_T != "0"):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"EQTRMT","Código":trafo.COD_ID,"erro":f"O código do faseamento do terciário possui valor não esperado. Valores esperados: AN, BN, CN.",
                "detalhamento":f"O código de fase do terciário (LIG_FAS_T) é: {trafo.LIG_FAS_T}. Alimentador: {trafo.CTMT}"})
            if (not _is_missing(trafo.TIP_TRAFO) and trafo.TIP_TRAFO in ['M','MT','B','T']
                    and not _is_missing(trafo.LIG_FAS_S) and trafo.LIG_FAS_S in ["A","B","C","AB","BC","CA","ABC"]
                    and not _is_missing(trafo.TEN_LIN_SE) and trafo.TEN_LIN_SE < 2.3):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"AVISO","Tabela":"EQTRMT","Código":trafo.COD_ID,"erro":f"O código do faseamento do secundário possui valor não esperado. A rede BT não possui neutro?",
                "detalhamento":f"Tipo do transformador: {trafo.TIP_TRAFO}, fase do primário (LIG_FAS_P):{trafo.LIG_FAS_P}, fase do secundário (LIG_FAS_S):{trafo.LIG_FAS_S}. Alimentador - {trafo.CTMT} "})
            try:
                if (((convert_tpotaprt(trafo.POT_NOM) > 100) and not _is_missing(trafo.TIP_TRAFO) and trafo.TIP_TRAFO != 'T')
                        or ((convert_tpotaprt(trafo.POT_NOM) > 300) and not _is_missing(trafo.TIP_TRAFO) and trafo.TIP_TRAFO == 'T')) and not _is_missing(trafo.TEN_LIN_SE) and trafo.TEN_LIN_SE < 2.3:
                    erros.append({"COD_BASE":self.cod_base,"Erro máx":"AVISO","Tabela":"EQTRMT","Código":trafo.COD_ID,"erro":f"A potência aparente nominal do transformador de distribuição possui valor não esperado. Valores esperados são inferiores a 300 para trifásicos e inferiores a 100 para monofásicos e bifásicos.",
                    "detalhamento":f"Potência declarada (POT_NOM):{convert_tpotaprt(trafo.POT_NOM)}, barras - PAC_1:{trafo.PAC_1}/PAC_2:{trafo.PAC_2}. Alimentador: {trafo.CTMT}"})
                if trafo.PER_TOT/(convert_tpotaprt(trafo.POT_NOM)*1000) > 0.05:
                    erros.append({"COD_BASE":self.cod_base,"Erro máx":"AVISO","Tabela":"EQTRMT","Código":trafo.COD_ID,"erro":f"O campo da perda potência total possui valor não esperado. Valores esperados são abaixo de 5%.",
                    "detalhamento":f"Perda total (PER_TOT) = {trafo.PER_TOT} W, {(trafo.PER_TOT / (convert_tpotaprt(trafo.POT_NOM) * 1000)) * 100:.2f}%. Alimentador - {trafo.CTMT}"})
            except ValueError:
                continue
                # print(f'Valor de potência ou perdas declarado errado: {convert_tpotaprt(trafo.POT_NOM)}kVA, {trafo.PER_TOT}W')
        return(pd.DataFrame(erros))

    def check_regulator(self,df_reg):
        """Faz a verificação dos parâmetros dos reguladores MT
        df_reg = dataframe da união do UNREMT e EQRE"""
        erros = []
        fas_con = ["A","B","C","AN","BN","CN","AX","BX","CX","AB","BC","CA","ABC","ABCN"]
        for reg in df_reg.itertuples():
            if _invalid_or_missing_enum(reg.LIG_FAS_P, fas_con) or _invalid_or_missing_enum(reg.LIG_FAS_S, fas_con):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"EQRE","Código":reg.COD_ID,"erro":f"O código do faseamento (primário ou secundário) possui um valor não esperado. Valores esperados são ABCN, ABC, AB, BC, CA, AN, BN, CN, A, B, C.",
                "detalhamento":f"Fase de primário:{reg.LIG_FAS_P},Fase de secundário:{reg.LIG_FAS_S}. Alimentador: {reg.CTMT}"})
            if reg.PER_TOT < reg.PER_FER:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"EQRE","Código":reg.COD_ID,"erro":f"O campo da perda potência ferro é maior que o campo perda potência total. Valores esperados são abaixo da perda potência total.",
                "detalhamento":f"O valor das perdas totais no regulador é de:{reg.PER_TOT},já o valor das perdas no ferro é de:{reg.PER_FER}. Alimentador: {reg.CTMT}"})
            if _invalid_or_missing_enum(reg.TIP_REGU, ['M','DA','T','DF']):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"UNREMT","Código":reg.COD_ID,"erro":f"O código do tipo de regulador possui um valor não esperado. Valores esperados são M, DA, T, DF.",
                "detalhamento":f"Tipo do regulador declarado (TIP_REGU):{reg.TIP_REGU}. Alimentador: {reg.CTMT}"})
            if (((convert_tpotaprt(reg.POT_NOM) < 1000) and not _is_missing(reg.TIP_REGU) and reg.TIP_REGU != 'T')
                    or ((convert_tpotaprt(reg.POT_NOM) < 3000) and not _is_missing(reg.TIP_REGU) and reg.TIP_REGU == 'T')):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"AVISO","Tabela":"EQRE","Código":reg.COD_ID,"erro":f"A potência aparente nominal do regulador possui um valor não esperado. Valores esperados são superiores a 1000kVA para reguladores monofásicos e 3000kVA para reguladores trifásicos.",
                "detalhamento":f"O valor da potência nominal do regulador é (POT_NOM):{convert_tpotaprt(reg.POT_NOM)}, tipo do regulador (TIP_REGU):{reg.TIP_REGU}. Alimentador: {reg.CTMT}"})
            if reg.PER_TOT > 7000:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"AVISO","Tabela":"EQRE","Código":reg.COD_ID,"erro":f"O campo da perda de potência total possui valor não esperado. Valores esperados são abaixo de 7kW.",
                "detalhamento":f"Perda total (PER_TOT):{reg.PER_TOT} W. Alimentador: {reg.CTMT}"})

        return(pd.DataFrame(erros))

    def check_ucmt(self):
        """Faz a verificação dos parâmetros das cargas MT"""
        erros = []
        lista_crvcrg = self.df['CRVCRG']['COD_ID'].tolist()

        for load in self.df['UCMT'].itertuples():
            if not _is_missing(load.FAS_CON) and load.FAS_CON in ['AC','ACN','CB','CBN','BA','BAN']:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"UCMT","Código":load.COD_ID,"erro":f"O código do faseamento foi declarado na sequência inversa. O programa espera ordem direta.",
                "detalhamento":f"A fase da carga MT (FAS_CON) declarada foi:{load.FAS_CON}. Alimentador: {load.CTMT}"})
            if _invalid_or_missing_enum(load.FAS_CON, ["A","B","C","AN","BN","CN","AX","BX","CX","AB","BC","CA","CAN","BCN","ABN","ABC","ABCN"]):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"UCMT","Código":load.COD_ID,"erro":f"O código do faseamento possui valor não esperado. Valores esperados são: ABCN,ABC,ABN,BCN,CAN,AB,BC,CA,AN,BN,CN,A,B,C.",
                "detalhamento":f"A fase da carga MT (FAS_CON) declarada foi:{load.FAS_CON}. Alimentador: {load.CTMT}"})
            if _is_missing(load.SEMRED) or load.SEMRED not in [0, 1]:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"UCMT","Código":load.COD_ID,"erro":f"O campo para identificar presença de rede possui valor não esperado. Valores esperados são 0 e 1.",
                "detalhamento":f"O atributo SEMRED da carga MT foi declarado como:{load.SEMRED}. Alimentador: {load.CTMT}"})
            if _is_missing(load.CTMT):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"UCMT","Código":load.COD_ID,"erro":f"O alimentador não foi declarado.",
                "detalhamento":f"O atributo CTMT da carga MT não foi declarado ({load.SEMRED})."})
            if _is_missing(load.TIP_CC) or load.TIP_CC not in lista_crvcrg:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0% CG","Tabela":"UCMT","Código":load.COD_ID,"erro":f"O código da curva de carga declarada na tabela CargaMT não apresenta correspondência com o declarado na tabela CrvCrgMT.",
                "detalhamento":f"O atributo TIP_CC da carga MT declarada não tem correspondência na entidade CRVCRG da BDGD analisada. Alimentador: {load.CTMT}"})

        return(pd.DataFrame(erros))

    def check_loadbt(self,load_type):
        """Faz a verificação dos parâmetros das cargas BT.
        load_type = UCBT(consumidores de BT) ou PIP(pontos de iluminação pública)"""
        erros = []
        lista_crvcrg = self.df['CRVCRG']['COD_ID'].tolist()

        if load_type == 'UCBT':
            ucbt = 'UCBT'
        else:
            ucbt = 'PIP'

        for load in self.df[ucbt].itertuples(index=False):
            if not _is_missing(load.FAS_CON) and load.FAS_CON in ['AC','ACN','CB','CBN','BA','BAN']:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":load_type,"Código":load.COD_ID,"erro":f"O código do faseamento foi declarado na sequência inversa. O programa espera ordem direta.",
                "detalhamento":f"A fase da carga BT (FAS_CON) declarada foi:{load.FAS_CON}. Alimentador: {load.CTMT}"})
            if _invalid_or_missing_enum(load.FAS_CON, ["A","B","C","AN","BN","CN","AX","BX","CX","AB","BC","CA","CAN","BCN","ABN","ABC","ABCN"]):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":load_type,"Código":load.COD_ID,"erro":f"O código do faseamento possui valor não esperado. Valores esperados são: ABCN,ABC,ABN,BCN,CAN,AB,BC,CA,AN,BN,CN,A,B,C.",
                "detalhamento":f"A fase da carga BT (FAS_CON) declarada foi:{load.FAS_CON}. Alimentador: {load.CTMT}"})
            if _is_missing(load.CTMT):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":load_type,"Código":load.COD_ID,"erro":f"O alimentador não foi declarado.",
                "detalhamento":f"O atributo CTMT da carga BT não foi declarado."})
            if _is_missing(load.TIP_CC) or load.TIP_CC not in lista_crvcrg:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0% CG","Tabela":load_type,"Código":load.COD_ID,"erro":f"O código da curva de carga declarada na tabela CargaBT não apresenta correspondência com o declarado na tabela CrvCrgBT.",
                "detalhamento":f"O atributo TIP_CC da carga BT declarada não tem correspondência na entidade CRVCRG da BDGD analisada. Alimentador: {load.CTMT}"})
            if _is_missing(load.UNI_TR_MT):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":load_type,"Código":load.COD_ID,"erro":f"O transformador não foi declarado.",
                "detalhamento":f"O atributo UNI_TR_MT da carga BT não foi declarado. Alimentador: {load.CTMT}"})
            if load_type == 'UCBT' and (_is_missing(load.SEMRED) or load.SEMRED not in [0, 1]):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":load_type,"Código":load.COD_ID,"erro":f"O campo para identificar presença de rede possui valor não esperado. Valores esperados são 0 e 1.",
                "detalhamento":f"O atributo SEMRED da carga BT foi declarado como:{load.SEMRED}. Alimentador: {load.CTMT}"})

        return(pd.DataFrame(erros))

    def check_energy(self,load_type):
        """verifica se existe alguma carga com energia zerada anualmente.
        load_type = UCBT para cargas de BT e UCMT para cargas de MT."""
        erros = []
        colunas = [f"ENE_{str(i).zfill(2)}" for i in range(1, 13)]
        df = self.df[load_type].copy()
        df = df.assign(SUM_ENE=df[colunas].sum(axis=1))
        dfx = df[(df['SUM_ENE'] == 0)]
        if load_type == 'UCMT':
            k = 100
        else:
            k = 2
        dfy = df[(df[colunas] > k*1000).any(axis=1)]

        if not dfy.empty:
            dfy = dfy.assign(colunas_acima_100=(dfy[colunas] > k*1000).apply(lambda row: list(row[row].index), axis=1))

        for load in dfx.itertuples():
            erros.append({"COD_BASE":self.cod_base,"Erro máx":"AVISO","Tabela":load_type,"Código":load.COD_ID,"erro":f"Não houve consumo da carga em nenhum mês?",
            "detalhamento":f"A soma das energias mensais (ENE_01 a ENE_12) é zero. Alimentador: {load.CTMT}"})
        for load in dfy.itertuples():
            erros.append({"COD_BASE":self.cod_base,"Erro máx":"AVISO","Tabela":load_type,"Código":load.COD_ID,"erro":f"Consumo da carga maior do que 100 MWh/mês?",
            "detalhamento":f"Coluna(s): {load.colunas_acima_100} com valor acima de {k} MWh. Alimentador: {load.CTMT}"})

        return(pd.DataFrame(erros))

    def check_ctmt(self):
        """Faz a verificação dos parâmetros dos circuitos de média tensão (alimentadores) - entidade CTMT"""
        erros = []

        for index,ctmt in self.df['CTMT'].iterrows():
            if _is_missing(ctmt['TEN_NOM']):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"CTMT","Código":ctmt['COD_ID'],"erro":f"A tensão nominal não foi declarada.",
                "detalhamento":f"O alimentador {ctmt['COD_ID']} não tem tensão nominal (TEN_NOM) declarada."})
            elif 2.3 > ValidadorBDGD.convert_ten(ctmt['TEN_NOM']) or ValidadorBDGD.convert_ten(ctmt['TEN_NOM']) > 48:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"CTMT","Código":ctmt['COD_ID'],"erro":f"A tensão nominal declarada possui valor não esperado. Valores esperados 2,3kV <= TenNom_kV <= 48,0kV.",
                "detalhamento":f"A tensão nominal (TEN_NOM) declarada do alimentador está fora dos limites estabelecidos: {ValidadorBDGD.convert_ten(ctmt['TEN_NOM'])} kV."})
            if _is_missing(ctmt['TEN_OPE']):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":"CTMT","Código":ctmt['COD_ID'],"erro":f"A tensão de operação não foi declarada.",
                "detalhamento":f"O alimentador {ctmt['COD_ID']} não tem tensão operacional (TEN_OPE) declarada."})
            if _is_missing(ctmt['SUB']):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"AVISO","Tabela":"CTMT","Código":ctmt['COD_ID'],"erro":f"Não foi declarado o código da subestação.",
                "detalhamento":f"O alimentador {ctmt['COD_ID']} não tem o campo SUB declarado."})
        return(pd.DataFrame(erros))

    def check_ctmt_energy(self):
        """Faz a verificação das energias dos alimentadores de média tensão da BDGD."""
        erros = []
        colunas = [f"ENE_{str(i).zfill(2)}" for i in range(1, 13)]
        df = self.df['CTMT'].copy()
        df = df.assign(
            SUM_ENE=df[colunas].sum(axis=1),
            PERD_total=df.loc[:, df.columns.str.startswith("PERD")].sum(axis=1) / 12000,
        )
        for i,ctmt in df.iterrows():
            if ctmt['SUM_ENE'] == 0 and ctmt['ATIP'] == 0:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"AVISO","Tabela":"CTMT","Código":ctmt['COD_ID'],"erro":f"Não houve consumo em nenhum mês? O alimentador não é atípico?",
                "detalhamento":f"A soma das energias mensais do alimentador (ENE_01 a ENE_12) é zero."})
            dict_ene = ValidadorBDGD.store_load_energy(self,feeder=ctmt['COD_ID'])
            for coluna in colunas:
                ene_load_gen = dict_ene[f"EnerMedidBT{coluna[4:]}_MWh"] + dict_ene[f"EnerMedidMT{coluna[4:]}_MWh"] - dict_ene[f"EnerMedidGenBT{coluna[4:]}_MWh"] - dict_ene[f"EnerMedidGenMT{coluna[4:]}_MWh"]
                if ctmt[coluna]/1000 < ene_load_gen and ctmt['RECONFIG'] == 0:
                    erros.append({"COD_BASE":self.cod_base,"Erro máx":"ERRO","Tabela":"CTMT","Código":ctmt['COD_ID'],"erro":f"Energia fornecida superior à energia injetada no mês de {ValidadorBDGD.convert_month(str(coluna[4:]))}",
                    "detalhamento":f"O valor do campo {coluna} é menor que a soma das energias das cargas menos as energias das gerações distribuídas do alimentador {ctmt['COD_ID']}"})
                if ctmt[coluna]/1000 < (ene_load_gen + ctmt['PERD_total']) and ctmt['RECONFIG'] == 0:
                    erros.append({"COD_BASE":self.cod_base,"Erro máx":"ERRO","Tabela":"CTMT","Código":ctmt['COD_ID'],"erro":f"Energia fornecida somada às perdas técnicas declaradas pela Distribuidora superior à energia injetada no mês de {ValidadorBDGD.convert_month(str(coluna[4:]))}",
                    "detalhamento":f"O valor do campo {coluna} é menor que a soma das energias das cargas, gerações distribuídas e das colunas que começam com 'PERD' do alimentador {ctmt['COD_ID']}."})
                try:
                    if 0.5 < ctmt[f"PNTBT_{coluna[4:]}"]/(ctmt[f"PNTBT_{coluna[4:]}"]+ctmt[f"PNTMT_{coluna[4:]}"])*(ctmt[coluna]/1000+ene_load_gen-2*dict_ene[f"EnerMedidBT{coluna[4:]}_MWh"]+ctmt['PERD_total'])/(dict_ene[f"EnerMedidBT{coluna[4:]}_MWh"]-dict_ene[f"EnerMedidGenBT{coluna[4:]}_MWh"]+ctmt["PERD_B"]+ctmt["PERD_MED"]) and ctmt['RECONFIG']:
                        erros.append({"COD_BASE":self.cod_base,"Erro máx":"AVISO","Tabela":"CTMT","Código":ctmt['COD_ID'],"erro":f"O percentual de perda não técnica supera 50% do mercado de baixa tensão somado às perdas técnicas declara pela Distribuidora no mês de {ValidadorBDGD.convert_month(str(coluna[4:]))}",
                        "detalhamento":f"A proporção do consumo irregular na baixa tensão - PNTBT_{coluna[4:]}/(PNTBT_{coluna[4:]}+PNTMT_{coluna[4:]}) pode indicar divergência nas medições."})
                except ZeroDivisionError:
                    continue
        return(pd.DataFrame(erros))

    def check_lines(self,line_type):
        """Faz a verificação dos parâmetros das linhas de média/baixa tensão e ramais de ligação.
        line_type = SSDMT(linhas de média tensão), SSDBT(linhas de baixa tensão) ou RAMLIG(ramais de ligação)"""

        if line_type == 'SSDMT':
            comp_max = 1000
        elif line_type == 'SSDBT':
            comp_max = 80
        else:
            comp_max = 30
        erros = []
        for line in self.df[line_type].itertuples():
            if not _is_missing(line.FAS_CON) and line.FAS_CON in ['AC','ACN','CB','CBN','BA','BAN']:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":line_type,"Código":line.COD_ID,"erro":f"O código do faseamento foi declarado na sequência inversa. O programa espera ordem direta.",
                "detalhamento":f"As fases da linha {line_type}:{line.COD_ID} (FAS_CON) declaradas foram:{line.FAS_CON}. Alimentador: {line.CTMT}"})
            elif _invalid_or_missing_enum(line.FAS_CON, ["A","B","C","AN","BN","CN","AX","BX","CX","AB","BC","CA","CAN","BCN","ABN","ABC","ABCN"]):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":line_type,"Código":line.COD_ID,"erro":f"O código do faseamento possui valor não esperado. Valores esperados são: ABCN,ABC,ABN,BCN,CAN,AB,BC,CA,AN,BN,CN,A,B,C.",
                "detalhamento":f"As fases da linha {line_type}:{line.COD_ID} (FAS_CON) declaradas foram:{line.FAS_CON}. Alimentador: {line.CTMT}"})
            if line.COMP == 0 or _is_missing(line.COMP):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0.5%","Tabela":line_type,"Código":line.COD_ID,"erro":f"O comprimento do {line_type} foi declarado igual a 0 ou não foi declarado.",
                "detalhamento":f"O comprimento (COMP) da linha {line_type}:{line.COD_ID} declarado foi:{line.COMP}. Alimentador: {line.CTMT}"})
            elif line.COMP > comp_max:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"AVISO","Tabela":line_type,"Código":line.COD_ID,"erro":f"O comprimento do {line_type} possui valor atípico. Valores típicos abaixo de {comp_max} m.",
                "detalhamento":f"O comprimento (COMP) da linha {line_type}:{line.COD_ID} declarado foi:{line.COMP}, sendo acima do valor máximo estabelecido. Alimentador: {line.CTMT}"})
            if _is_missing(line.TIP_CND):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":line_type,"Código":line.COD_ID,"erro":f"O condutor utilizado no {line_type} não foi declarado.",
                "detalhamento":f"O parâmetro TIP_CND da tabela {line_type} não foi declarado. Alimentador: {line.CTMT}"})
            elif not _is_missing(line.TIP_CND) and line.TIP_CND not in self.df['SEGCON']['COD_ID'].tolist():
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":line_type,"Código":line.COD_ID,"erro":f"O código do condutor declarado na tabela {line_type} não apresenta correspondência com o declarado na tabela CodCondutor",
                "detalhamento":f"O parâmetro TIP_CND do {line_type} ({line.TIP_CND}) não faz parte dos tipos de condutores declarados dessa BDGD (SEGCON). Alimentador: {line.CTMT}"})
            if _is_missing(line.CTMT):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"2%","Tabela":line_type,"Código":line.COD_ID,"erro":f"O alimentador não foi declarado.",
                "detalhamento":f"O atributo CTMT do {line_type} não foi declarado."})
            if _invalid_or_missing_enum(line.ARE_LOC, ['UB','NU']):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":line_type,"Código":line.COD_ID,"erro":f"A classificação do segmento possui um valor não esperado. Valores esperados são UB e NU.",
                "detalhamento":f"O campo 'ARE_LOC' foi declarado como: {line.ARE_LOC}. Alimentador: {line.CTMT}"})

        return(pd.DataFrame(erros))

    def check_unse(self,chave):
        """Faz a verificação dos parâmetros elétricos das chaves de MT e BT da BDGD.
        chave = deve ser UNSEMT(chaves de média tensão) ou UNSEBT (chaves de baixa tensão)"""
        erros = []
        for unse in self.df[chave].itertuples():
            if not _is_missing(unse.FAS_CON) and unse.FAS_CON in ['AC','ACN','CB','CBN','BA','BAN']:
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":chave,"Código":unse.COD_ID,"erro":f"O código do faseamento foi declarado na sequência inversa. O programa espera ordem direta.",
                "detalhamento":f"As fases da chave de média/baixa tensão (FAS_CON) declaradas foram:{unse.FAS_CON}. Alimentador: {unse.CTMT}"})
            elif _invalid_or_missing_enum(unse.FAS_CON, ["A","B","C","AN","BN","CN","AX","BX","CX","AB","BC","CA","CAN","BCN","ABN","ABC","ABCN"]):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"0%","Tabela":chave,"Código":unse.COD_ID,"erro":f"O código do faseamento possui valor não esperado. Valores esperados são: ABCN,ABC,ABN,BCN,CAN,AB,BC,CA,AN,BN,CN,A,B,C.",
                "detalhamento":f"As fases da chave de média/baixa tensão (FAS_CON) declaradas foram:{unse.FAS_CON}. Alimentador: {unse.CTMT}"})
            if _is_missing(unse.CTMT):
                erros.append({"COD_BASE":self.cod_base,"Erro máx":"2%","Tabela":chave,"Código":unse.COD_ID,"erro":f"O alimentador não foi declarado.",
                "detalhamento":f"O atributo CTMT da chave não foi declarado."})

        return(pd.DataFrame(erros))

    def return_graph_trafo(df,df_bt,trafo):
        """Retorna o grafo em sequencia do transformador BT escolhido"""
        transformador = trafo
        pac_trafo = df.at[df[df['COD_ID'] == trafo].index[0],'PAC_2']

        df_line_bt = df_bt.query("UNI_TR_MT == @transformador")

        grafo = nx.Graph()

        for row in df_line_bt.itertuples(index=False):
            if row.ELEM == 'UCBT':
                grafo.add_node(row.PAC_1,cod_id=row.COD_ID, fas_con=row.FAS_CON, tipo=row.ELEM)
            else:
                grafo.add_node(row.PAC_1, tipo='bus')
            grafo.add_node(row.PAC_2)
            grafo.add_edge(row.PAC_1, row.PAC_2, cod_id=row.COD_ID, fas_con=row.FAS_CON, tipo=row.ELEM)

        conectados = list(nx.connected_components(grafo))

        if any(pac_trafo in grf for grf in conectados):
            sequencia = list(nx.bfs_edges(grafo,pac_trafo)) 
            return(sequencia,grafo)
        else:
            return(None, None)

    def store_load_energy(self,feeder):
        """Armazena as energias das cargas de BT e MT para cada alimentador.
        feeder = CTMT analisado"""

        alimentador = feeder
        columns = ['CTMT']

        for n in range(1,13):
            columns.append(f"ENE_{n:02d}")

        df_bt = pd.concat([self.df['UCBT'][columns].query('CTMT == @alimentador'),self.df['PIP'][columns].query('CTMT == @alimentador')],ignore_index=True)
        df_mt = self.df['UCMT'][columns].query('CTMT == @alimentador')
        df_gdbt = self.df['UGBT'][columns].query('CTMT == @alimentador')
        df_gdmt = self.df['UGMT'][columns].query('CTMT == @alimentador')

        data = {}

        for n in range(1,13):
            data[f"EnerMedidBT{n:02d}_MWh"] = float(df_bt[f"ENE_{n:02d}"].sum())/1000
            data[f"EnerMedidMT{n:02d}_MWh"] = float(df_mt[f"ENE_{n:02d}"].sum())/1000
            data[f"EnerMedidGenBT{n:02d}_MWh"] = float(df_gdbt[f"ENE_{n:02d}"].sum())/1000
            data[f"EnerMedidGenMT{n:02d}_MWh"] = float(df_gdmt[f"ENE_{n:02d}"].sum())/1000

        return(data)

    def convert_month(key):
        switch_dict = {
            "01": "janeiro",
            "02": "fevereiro",
            "03": "março",
            "04": "abril",
            "05": "maio",
            "06": "junho",
            "07": "julho",
            "08": "agosto",
            "09": "setembro",
            "10": "outubro",
            "11": "novembro",
            "12": "dezembro"
        }
        return(switch_dict[key])

    def convert_ten(key):
        switch_dict = {
            "0": 0.0,
            "1": 0.11,
            "2": 0.115,
            "3": 0.120,
            "4": 0.121,
            "5": 0.125,
            "6": 0.127,
            "7": 0.208,
            "8": 0.216,
            "9": 0.2165,
            "10": 0.220,
            "11": 0.230,
            "12": 0.231,
            "13": 0.240,
            "14": 0.254,
            "15": 0.380,
            "16": 0.400,
            "17": 0.440,
            "18": 0.480,
            "19": 0.500,
            "20": 0.600,
            "21": 0.750,
            "22": 1.0,
            "23": 2.3,
            "24": 3.2,
            "25": 3.6,
            "26": 3.785,
            "27": 3.8,
            "28": 3.848,
            "29": 3.985,
            "30": 4.160,
            "31": 4.2,
            "32": 4.207,
            "33": 4.368,
            "34": 4.560,
            "35": 5,
            "36": 6,
            "37": 6.6,
            "38": 6.93,
            "39": 7.96,
            "40": 8.67,
            "103": 11,
            "41": 11.4,
            "104": 11.5,
            "42": 11.9,
            "43": 12.0,
            "44": 12.6,
            "45": 12.7,
            "105": 13,
            "46": 13.2,
            "47": 13.337,
            "48": 13.530,
            "49": 13.8,
            "50": 13.86,
            "51": 14.14,
            "52": 14.19,
            "53": 14.4,
            "54": 14.835,
            "55": 15,
            "56": 15.2,
            "57": 19.053,
            "58": 19.919,
            "106": 20,
            "59": 21,
            "60": 21.5,
            "61": 22,
            "62": 23,
            "63": 23.1,
            "64": 23.827,
            "65": 24,
            "66": 24.2,
            "67": 25,
            "68": 25.8,
            "69": 27,
            "70": 30,
            "71": 33,
            "72": 34.5,
            "73": 36,
            "74": 38,
            "75": 40,
            "76": 44,
            "77": 45,
            "78": 45.4,
            "79": 48,
            "80": 60,
            "81": 66,
            "107": 68,
            "82": 69,
            "83": 72.5,
            "108": 85,
            "84": 88,
            "85": 88.2,
            "86": 92,
            "87": 100,
            "88": 120,
            "89": 121,
            "90": 123,
            "91": 131.6,
            "92": 131.630,
            "93": 131.635,
            "94": 138,
            "95": 145,
            "96": 230,
            "97": 345,
            "109": 440,
            "98": 500,
            "99": 750,
            "100": 1000,
            "101": 245,
            "102": 550
        }
        return(switch_dict[key])

    def find_seq(sequencia,index):
        atual = sequencia[index]
        anterior = sequencia[index-1]

        # Se a sequência quebrou
        if atual[0] != anterior[1]:
            #print(f"\nSequência quebrou na tupla {atual}. Procurando correspondência atrás...")

            # Procurar a primeira tupla anterior onde seq[1] == atual[0]
            for j in range(index-1, -1, -1):
                if sequencia[j][1] == atual[0]:
                    #print(f"→ Encontrada correspondência com a tupla {sequencia[j]} (posição {j})")
                    return(sequencia[j])
            else:
                pass
                # print("→ Nenhuma tupla anterior satisfaz a condição.")
        else:
            return(anterior)


    def exportar_erros_excel(self, df: pd.DataFrame, output_folder: str, feeder: Optional[str] = ""):
        """Criação do arquivo de erros em formato xlsx
        df = dataframe consolidado dos erros encontrados na verificação
        output_folder = pasta de saída
        """
        path = os.path.join(output_folder, f'{feeder}_etapa17_{self.cod_base}.xlsx')

        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:

            # === ABA 1: LISTA COMPLETA DE ERROS ===
            df.to_excel(writer, index=False, sheet_name="Erros")

            workbook  = writer.book
            worksheet = writer.sheets["Erros"]

            # Ajuste de largura das colunas
            worksheet.set_column("A:A", 12)   # COD_BASE
            worksheet.set_column("B:B", 10)   # Erro máx
            worksheet.set_column("C:C", 12)   # Tabela
            worksheet.set_column("D:D", 15)   # Código
            worksheet.set_column("E:E", 120)  # erro
            worksheet.set_column("F:F", 120)  # detalhamento

            # Formatação cabeçalho
            header_format = workbook.add_format({
                "bold": True,
                "bg_color": "#D0D0D0",
                "border": 1
            })

            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)

            # === ABA 2: RESUMO AGRUPADO ===
            resumo = (
            df.groupby(["COD_BASE", "Erro máx","Tabela", "erro"])
                .size()
                .reset_index(name="quantidade")
                .sort_values(["COD_BASE","Erro máx","Tabela", "erro"])
            )
            erropercentual = []
            for line in resumo.itertuples():
                erropercentual.append(f'{line.quantidade/len(self.df[line.Tabela])*100:.2f}%')
            resumo = resumo.assign(**{'erro atual': erropercentual})
            resumo.to_excel(writer, index=False, sheet_name="Resumo")

            worksheet2 = writer.sheets["Resumo"]

            # Ajustes de largura da aba resumo
            worksheet2.set_column("A:A", 12)   # COD_BASE
            worksheet2.set_column("B:B", 10)   # Erro máx
            worksheet2.set_column("A:A", 12)   # Tabela
            worksheet2.set_column("B:B", 120)  # erro
            worksheet2.set_column("C:C", 12)   # quantidade

            # Cabeçalho da aba resumo
            for col_num, value in enumerate(resumo.columns.values):
                worksheet2.write(0, col_num, value, header_format)

        _report_verification("export", f"Arquivo Excel exportado: {path}")

    def exportar_scan_excel(self, df: pd.DataFrame, output_folder: str, feeder: Optional[str] = ""):
        """Criação do arquivo de erros em formato xlsx para a pré-validação da BDGD
        df = dataframe consolidado dos erros encontrados na verificação
        output_folder = pasta de saída
        """
        path = os.path.join(output_folder, f'{feeder}_scan_{self.cod_base}.xlsx')

        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:

            # === ABA 1: LISTA COMPLETA DE ERROS ===
            df.to_excel(writer, index=False, sheet_name="Erros")

            workbook  = writer.book
            worksheet = writer.sheets["Erros"]

            # Ajuste de largura das colunas
            worksheet.set_column("A:A", 12)   # COD_BASE
            worksheet.set_column("B:B", 10)   # Erro máx
            worksheet.set_column("C:C", 12)   # Tabela
            worksheet.set_column("D:D", 15)   # Código
            worksheet.set_column("E:E", 10)   # Índice
            worksheet.set_column("F:F", 120)  # erro
            worksheet.set_column("G:G", 120)  # detalhamento

            # Formatação cabeçalho
            header_format = workbook.add_format({
                "bold": True,
                "bg_color": "#D0D0D0",
                "border": 1
            })

            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)

        _report_verification("export", f"Arquivo Excel de scan exportado: {path}")

    def check_propagacao(self,dataframe: Optional[gpd.geodataframe.GeoDataFrame] = None, lista_isolados:list = []): #Criar um dicionário de tensão por nó para verificar as tensões nos SSDMT,SSDBT,CHVMT,CHVBT e REGUL
        """Faz o faseamento de todos os elementos do alimentador específico para verificar se há alguma inconsistência entre o faseamento 
        dos elementos (exemplo de erro: fase do elemento anterior AB e fase do elemento atual ABC)
        dataframe = geodataframe completo da BDGD"""
        isolados = lista_isolados
        #mandar tambem o iso_trafos de elementos que estão nos trafos errados para jogar aqui em caso de não estarem isolados e não serem englobados no voltage_dict
        erros = []

        ctmt_feeders = list(enumerate(dataframe['CTMT']['COD_ID'].tolist()))
        feeder_iterator = tqdm(ctmt_feeders, desc="Propagacao de faseamento", unit=" alimentador", ncols=100)
        for i, feeder in feeder_iterator:
            fase_error = {}
            fase_error_bt = {}
            grafos = []
            pacs_trf = []
            feeder_iterator.set_description(f"Propagacao de faseamento: {feeder}")

            if dataframe['CTMT'].at[i,'ATIP'] == 1:
                erromax = ['0.5%',' em circuito atípico ']
            else:
                erromax = ['0%',' ']

            alimentador = feeder
            df_trafo = inner_entities_tables(dataframe['EQTRMT'], dataframe['UNTRMT'].query("CTMT==@alimentador"),
                                    left_column='UNI_TR_MT', right_column='COD_ID')
            df_regul = inner_entities_tables(dataframe['EQRE'], dataframe['UNREMT'].query("CTMT==@alimentador"),
                                                left_column='UN_RE', right_column='COD_ID')
            df_tr_mtmt = assign_elem(
                df_trafo[df_trafo['TEN_LIN_SE'] > 1][['COD_ID','CTMT','PAC_1','PAC_2','LIG_FAS_P','LIG_FAS_S','TEN_LIN_SE']],
                'EQTRMT',
            ).rename(columns={'LIG_FAS_S':'FAS_CON'})
            df_aux_trafo = (
                df_trafo[['COD_ID','CTMT','PAC_1','PAC_2','LIG_FAS_P','LIG_FAS_S','LIG_FAS_T','TEN_LIN_SE']]
                .rename(columns={'LIG_FAS_P':'FAS_CON'})
                .assign(ELEM='EQTRMT')
            )
            df_aux_ssdmt = assign_elem(dataframe['SSDMT'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2','FAS_CON']], 'SSDMT')
            df_aux_ssdbt = assign_elem(dataframe['SSDBT'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2','FAS_CON','UNI_TR_MT']], 'SSDBT')
            df_aux_ramalig = assign_elem(dataframe['RAMLIG'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2','FAS_CON','UNI_TR_MT']], 'RAMLIG')
            df_aux_unsemt = assign_elem(dataframe['UNSEMT'].query("CTMT == @alimentador & P_N_OPE == 'F'")[['COD_ID','CTMT','PAC_1','PAC_2','FAS_CON']], 'UNSEMT')
            df_aux_unsebt = assign_elem(dataframe['UNSEBT'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2','FAS_CON']], 'UNSEBT')
            df_aux_regul = assign_elem(df_regul[['COD_ID','CTMT','PAC_1','PAC_2','FAS_CON']], 'EQRE')
            df_ucmt = dataframe['UCMT'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC','FAS_CON']]
            df_aux_ucmt = (
                pd.DataFrame(df_ucmt).groupby('COD_ID', as_index=False)
                .agg({'COD_ID':'last','CTMT':'last','PAC':'last','FAS_CON':'last'})
                .rename(columns={'PAC':'PAC_1'})
                .assign(PAC_2='', ELEM='UCMT')
            )
            df_ucbt = dataframe['UCBT'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC','FAS_CON','UNI_TR_MT']]
            df_aux_ucbt = (
                pd.DataFrame(df_ucbt).groupby('COD_ID', as_index=False)
                .agg({'COD_ID':'last','CTMT':'last','PAC':'last','FAS_CON':'last','UNI_TR_MT':'last'})
                .rename(columns={'PAC':'PAC_1'})
                .assign(PAC_2='', ELEM='UCBT')
            )
            df_aux_pip = (
                dataframe['PIP'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC','FAS_CON']]
                .rename(columns={'PAC':'PAC_1'})
                .assign(PAC_2='', ELEM='PIP')
            )

            df_elements = pd.concat([df_aux_ssdmt,df_aux_unsemt,df_aux_regul,df_tr_mtmt], ignore_index=True)#para faseamento de linhas
            df_aux_ucmt_tr = pd.concat([df_aux_ucmt,df_aux_trafo],ignore_index=True) #para faseamento de cargas e trafos
            df_elements_bt = pd.concat([df_aux_ssdbt,df_aux_ramalig],ignore_index=True) #para faseamento de linhas de bt
            df_aux_ucbt_pip = pd.concat([df_aux_ucbt,df_aux_pip],ignore_index=True) #para faseamento de cargas bt

            df = dataframe['CTMT'][['COD_ID','PAC_INI']]
            pac_ctmt = df.at[df[df['COD_ID'] == alimentador].index[0],'PAC_INI']

            grafo = nx.Graph()

            for row in df_elements.itertuples(index=False):
                grafo.add_node(row.PAC_1)
                grafo.add_node(row.PAC_2)
                #TODO 
                grafo.add_edge(row.PAC_1, row.PAC_2, cod_id=row.COD_ID,fas_con=row.FAS_CON, tipo=row.ELEM)
            try:
                grafo.remove_node('')
            except:
                pass
            conectados = list(nx.connected_components(grafo))

            if any(pac_ctmt in grf for grf in conectados):
                sequencia = list(nx.bfs_edges(grafo,pac_ctmt)) 
            else:
                # print(f"Não é possível gerar a sequência elétrica, pois o alimentador {feeder} não tem conexão com a fonte")
                continue
            if sequencia[0][0] in df_elements['PAC_1'].tolist():
                pac1 = 'PAC_1'
                pac2 = 'PAC_2'
            else: #sequencia inversa
                pac1 = 'PAC_2'
                pac2 = 'PAC_1'
            #verificação de faseamento das linhas,chaves e reguladores de MT
            for seq in sequencia:
                if seq[0] == pac_ctmt:
                    continue
                else:
                    try:
                        i_ele = df_elements.index[(df_elements[pac1] == seq[0]) & (df_elements[pac2] == seq[1])].tolist()[0] #índice do elemento atual
                    except IndexError: #elemento que não está seguindo a mesma ordem dos outros
                        i_ele = df_elements.index[(df_elements[pac2] == seq[0]) & (df_elements[pac1] == seq[1])].tolist()[0]
                    cod_ele = df_elements.at[i_ele,'COD_ID'] #código do elemento atual
                    if df_elements.at[i_ele,'ELEM'] == 'EQTRMT':
                        fase_ele = df_elements.at[i_ele,'FAS_CON']
                    else:
                        fase_ele = df_elements.at[i_ele,'FAS_CON'] #fase do elemento atual
                        seqx = ValidadorBDGD.find_seq(sequencia=sequencia, index=sequencia.index(seq))
                        if not df_elements.index[(df_elements[pac1] == seqx[0]) & (df_elements[pac2] == seqx[1])].empty:
                            i = df_elements.index[(df_elements[pac1] == seqx[0]) & (df_elements[pac2] == seqx[1])][0] #índice do elemento anterior
                        else:
                            i = df_elements.index[(df_elements[pac1] == seqx[1]) & (df_elements[pac2] == seqx[0])][0] #elemento com ordem de PAC trocada
                        fase = df_elements.at[i,'FAS_CON'] #fase do elemento anterior
                        if set(fase_ele) <= set(fase+'N'): #verificar se a fase do elemento atual está contida no elemento anterior sem importar a ordem
                            continue #faseamento correto
                        else:
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":erromax[0], "Tabela":df_elements.at[i_ele,'ELEM'], "Código":cod_ele,"erro":f"Elemento com propagação de fase inadequada{erromax[1]}após sequenciamento elétrico.", 
                            "detalhamento": f"Elemento analisado = {df_elements.at[i_ele,'ELEM']}:{cod_ele} - fase:{fase_ele}, Elemento de conexão - {df_elements.at[i,'ELEM']}:{df_elements.at[i,'COD_ID']}. Faseamento possível - combinação de:{fase}. Alimentador:{df_elements.at[i_ele,'CTMT']}."})
                            df_elements.loc[i_ele,'FAS_CON'] = fase
                            fase_error[cod_ele] = fase_ele
            #verificação do faseamento nos transformadores e nas cargas de média tensão
            for elem in df_aux_ucmt_tr.itertuples(index=False):
                if elem.COD_ID in isolados: #remove os elementos isolados da checagem de faseamento
                    continue
                indices = df_elements.index[(df_elements[pac2] == elem.PAC_1)|(df_elements[pac1] == elem.PAC_1)].tolist()
                if len(indices) == 0: #elemento conectado no nível de tensão errado
                    continue
                if len(indices) == 1:
                    fase = df_elements.at[indices[0],'FAS_CON']
                    if set(elem.FAS_CON) <= set(fase+'N'):
                        continue #faseamento correto
                    else:
                        erros.append({"COD_BASE": self.cod_base, "Erro máx":erromax[0], "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Elemento com propagação de fase inadequada{erromax[1]}após sequenciamento elétrico.",
                        "detalhamento":f"Elemento analisado = {elem.ELEM}:{elem.COD_ID} - fase:{elem.FAS_CON}, Elemento de conexão - {df_elements.at[i,'ELEM']}:{df_elements.at[i,'COD_ID']}. Fase de conexão - {fase}. Alimentador: {elem.CTMT}."})
                else:
                    find = False
                    for i in indices:
                        fase = df_elements.at[i,'FAS_CON']
                        if set(elem.FAS_CON) <= set(fase+'N'):
                            find = True
                            break
                    if find:
                        continue
                    erros.append({"COD_BASE": self.cod_base, "Erro máx":erromax[0], "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Elemento com propagação de fase inadequada{erromax[1]}após sequenciamento elétrico.",
                    "detalhamento":f"Elemento analisado = {elem.ELEM}:{elem.COD_ID} - fase:{elem.FAS_CON}, Elemento de conexão - {df_elements.at[i,'ELEM']}:{df_elements.at[i,'COD_ID']}. Fase de conexão - {fase}. Alimentador: {elem.CTMT}."})
            #faseamento das linhas de baixa tensão
            for trafo in df_aux_trafo['COD_ID'].tolist():
                if _is_missing(trafo):
                    # print('trafo sem COD_ID')
                    continue
                sequencia,grafo_bt = ValidadorBDGD.return_graph_trafo(df_aux_trafo,df_elements_bt,trafo)
                grafos.append(grafo_bt)
                if sequencia:
                    pac_trafo = sequencia[0][0]
                    pacs_trf.append(pac_trafo)
                    for seq in sequencia:
                        try:
                            i_ele = df_elements_bt.index[(df_elements_bt[pac1] == seq[0]) & (df_elements_bt[pac2] == seq[1])].tolist()[0] #índice do elemento atual
                        except IndexError:
                            i_ele = df_elements_bt.index[(df_elements_bt[pac1] == seq[1]) & (df_elements_bt[pac2] == seq[0])].tolist()[0]
                        cod_ele = df_elements_bt.at[i_ele,'COD_ID'] #código do elemento atual
                        fase_ele = df_elements_bt.at[i_ele,'FAS_CON'] #fase do elemento atual
                        if seq[0] == pac_trafo: #linha que está conectada ao transformador
                            i = df_aux_trafo.index[df_aux_trafo['COD_ID'] == trafo].tolist()[0]
                            fase = df_aux_trafo.at[i,'LIG_FAS_S']+df_aux_trafo.at[i,'LIG_FAS_T'] #fase do elemento anterior
                        else:
                            seqx = ValidadorBDGD.find_seq(sequencia=sequencia, index=sequencia.index(seq)) #acha o primeiro elemento anterior no grafo
                            if not df_elements_bt.index[(df_elements_bt[pac1] == seqx[0]) & (df_elements_bt[pac2] == seqx[1])].empty:
                                i = df_elements_bt.index[(df_elements_bt[pac1] == seqx[0]) & (df_elements_bt[pac2] == seqx[1])].tolist()[0]
                            else:
                                i = df_elements_bt.index[(df_elements_bt[pac1] == seqx[1]) & (df_elements_bt[pac2] == seqx[0])].tolist()[0]
                            fase = df_elements_bt.at[i,'FAS_CON'] #fase do elemento anterior
                        if set(fase_ele) <= set(fase+'N'): #verificar se a fase do elemento atual está contida no elemento anterior sem importar a ordem
                            continue #faseamento correto
                        else:
                            erros.append({"COD_BASE": self.cod_base, "Erro máx":erromax[0], "Tabela":df_elements_bt.at[i_ele,'ELEM'], "Código":df_elements_bt.at[i_ele,'COD_ID'],"erro":f"Elemento com propagação de fase inadequada{erromax[1]}após sequenciamento elétrico.",
                            "detalhamento":f"Elemento analisado = {df_elements_bt.at[i_ele,'ELEM']}:{df_elements_bt.at[i_ele,'COD_ID']} - fase:{fase_ele}, Elemento de conexão = {df_elements_bt.at[i,'ELEM']}:{df_elements_bt.at[i,'COD_ID']}. Fase de conexão - {fase}. Alimentador: {df_elements_bt.at[i_ele,'CTMT']}."})
                            df_elements_bt.loc[i_ele,'FAS_CON'] = fase
                            fase_error_bt[df_elements_bt.at[i_ele,'COD_ID']] = fase_ele
                else:
                    i = df_aux_trafo[df_aux_trafo['COD_ID'] == trafo].index[0]
                    # print(f'trafo {trafo} sem linhas de baixa tensão ou cargas!')
            #faseamento das cargas de baixa tensão
            for elem in df_aux_ucbt_pip.itertuples(index=False):
                if elem.COD_ID in lista_isolados:#remove os elementos isolados da checagem de faseamento
                    continue
                i_tr = df_aux_trafo.index[(df_aux_trafo['PAC_2'] == elem.PAC_1)].tolist()
                if len(i_tr) > 0:
                    if set(elem.FAS_CON) <= set(str(df_aux_trafo.loc[i_tr,'LIG_FAS_S']+'N')) or set(elem.FAS_CON) <= set(str(df_aux_trafo.loc[i_tr,'LIG_FAS_T']+'N')):
                        continue #faseamento correto
                    else:
                        erros.append({"COD_BASE": self.cod_base, "Erro máx":erromax[0], "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Elemento com propagação de fase inadequada{erromax[1]}após sequenciamento elétrico.",
                        "detalhamento":f"Elemento analisado = {elem.ELEM}:{elem.COD_ID} - fase:{elem.FAS_CON}, Elemento de conexão - {df_elements_bt.at[i,'ELEM']}:{df_elements_bt.at[i,'COD_ID']}. Fase de conexão - {fase}. Alimentador: {elem.CTMT}."})
                indices = df_elements_bt.index[(df_elements_bt['PAC_2'] == elem.PAC_1)|(df_elements_bt['PAC_1'] == elem.PAC_1)].tolist()
                if len(indices) == 1:
                    i = indices[0]
                    fase = df_elements_bt.at[i,'FAS_CON']
                    if set(elem.FAS_CON) <= set(fase+'N'):
                        continue #faseamento correto
                    else:
                        erros.append({"COD_BASE": self.cod_base, "Erro máx":erromax[0], "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Elemento com propagação de fase inadequada{erromax[1]}após sequenciamento elétrico.",
                        "detalhamento":f"Elemento analisado = {elem.ELEM}:{elem.COD_ID} - fase:{elem.FAS_CON}, Elemento de conexão - {df_elements_bt.at[i,'ELEM']}:{df_elements_bt.at[i,'COD_ID']}. Fase de conexão - {fase}. Alimentador: {elem.CTMT}."})
                elif len(indices) > 1:
                    find = False
                    for i in indices:
                        fase = df_elements_bt.at[i,'FAS_CON']
                        if set(elem.FAS_CON) <= set(fase+'N'):
                            find = True
                            break
                    if find:
                        continue
                    erros.append({"COD_BASE": self.cod_base, "Erro máx":erromax[0], "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Elemento com propagação de fase inadequada{erromax[1]}após sequenciamento elétrico.",
                    "detalhamento":f"Elemento analisado = {elem.ELEM}:{elem.COD_ID} - fase:{elem.FAS_CON}, Elemento de conexão - {df_elements_bt.at[i,'ELEM']}:{df_elements_bt.at[i,'COD_ID']}. Fase de conexão - {fase}. Alimentador: {elem.CTMT}."})
                else:#se não tiver num ramal ou linha de BT está em um trafo
                    try:
                        i = df_aux_trafo.index[df_aux_trafo['PAC_2'] == elem.PAC_1].tolist()[0]
                    except:
                        i = df_aux_trafo.index[df_aux_trafo['PAC_1'] == elem.PAC_1].tolist()[0]
                        # print(f'Carga BT {elem.COD_ID} conectada no primário do trafo {df_aux_trafo.at[i,'COD_ID']}!!')
                    fase = df_elements_bt.at[i,'FAS_CON']
                    if set(elem.FAS_CON) <= set(fase+'N'):
                        continue #faseamento correto
                    else:
                        erros.append({"COD_BASE": self.cod_base, "Erro máx":erromax[0], "Tabela":elem.ELEM, "Código":elem.COD_ID, "erro":f"Elemento com propagação de fase inadequada{erromax[1]}após sequenciamento elétrico.",
                        "detalhamento":f"Elemento analisado = {elem.ELEM}:{elem.COD_ID} - fase:{elem.FAS_CON}, Elemento de conexão - {df_elements_bt.at[i,'ELEM']}:{df_elements_bt.at[i,'COD_ID']}. Fase de conexão - {fase}. Alimentador: {elem.CTMT}."})
            if self.path_coords:
                df_mt = pd.concat([df_elements,df_aux_ucmt_tr],ignore_index=True)
                df_bt = pd.concat([df_elements_bt,df_aux_ucbt],ignore_index=True)
                self.plot_fase_error(df_mt=df_mt,df_bt=df_bt,fase_error=fase_error,fase_error_bt=fase_error_bt,pac_ctmt=pac_ctmt,
                                    feeder=feeder,path_coords=self.path_coords, output_path=self.output_folder)
        return(pd.DataFrame(erros))
    
    def plot_isolated_circuits(self, df_total,pac_ctmt,df_isolated,feeder,output_path): #cria um plot dos elementos isolados
        #cria um arquivo HTML com os elementos isolados em destaque, caso exista. 
        df = pd.read_csv(self.path_coords)
        pos = {
            _pac_key(row["PAC"]): (row["long"], row["lat"])
            for _, row in df.iterrows()
            if _pac_key(row["PAC"]) is not None
        }
        iso_traces = self.iso_points(df_isolated=df_isolated,pos=pos) #desenho dos elementos isolados
        regular_traces = self.graph_points(df_total=df_total,pos=pos)
        highlight_trace = None
        pac_ctmt_key = _pac_key(pac_ctmt)
        if pac_ctmt_key in pos:
            x, y = pos[pac_ctmt_key]
            highlight_trace = go.Scatter(
                x=[x],
                y=[y],
                mode='markers',
                marker=dict(
                    size=15,
                    color='black',
                    symbol='triangle-up'),
                text=[f'PAC_INI:{pac_ctmt_key}<br>'
                      f'Alimentador:{feeder}'],
                hoverinfo='text',
                name='SE')
        traces = regular_traces+iso_traces+[highlight_trace]
        fig = go.Figure(data=traces)
        fig.add_annotation(
            x=0,
            y=1.05,
            xref="paper",
            yref="paper",
            text=f"Alimentador:{feeder} - Elementos Isolados",
            showarrow=False)
        fig_path = os.path.join(output_path, f"figuras_{feeder}")

        if not os.path.exists(fig_path):
            os.makedirs(fig_path)
        path = os.path.join(fig_path,f"isolated_{feeder}.html")

        fig.write_html(path)
    
    def graph_points(self,df_total,pos):
        grafo = nx.Graph()
        for row in df_total.itertuples(index=False):
            p1 = _pac_key(row.PAC_1)
            p2 = _pac_key(row.PAC_2)
            if p1 is None or p2 is None:
                continue
            if row.ELEM == 'UCBT' or row.ELEM == 'UCMT':
                grafo.add_node(p1,cod_id=row.COD_ID,tipo=row.ELEM)
            elif row.ELEM == 'SSDBT' or row.ELEM == 'RAMLIG':
                grafo.add_node(p1,tipo='BT')
                grafo.add_node(p2,tipo='BT')
                grafo.add_edge(p1, p2, cod_id=row.COD_ID,tipo=row.ELEM)
            else:
                grafo.add_node(p1,tipo='MT')
                if row.ELEM == 'UNTRMT':
                    grafo.add_node(p2,cod_id=row.COD_ID,tipo='UNTRMT')#TODO continuar
                else:
                    grafo.add_node(p2,tipo='MT')
                grafo.add_edge(p1, p2, cod_id=row.COD_ID,tipo=row.ELEM)
        try:
            grafo.remove_node('')
        except:
            pass
        valid_nodes = [n for n in grafo.nodes if n in pos]
        subgrafo = grafo.subgraph(valid_nodes)
        # Coordenadas das linhas
        edge_x_mt = []
        edge_y_mt = []
        edge_x_bt = []
        edge_y_bt = []

        for u, v, data in subgrafo.edges(data=True):
            x0, y0 = pos[u]
            x1, y1 = pos[v]
            if data['tipo'] == 'SSDBT' or data['tipo'] == 'RAMLIG':
                edge_x_bt.extend([x0, x1, None])
                edge_y_bt.extend([y0, y1, None])
            else:
                edge_x_mt.extend([x0, x1, None])
                edge_y_mt.extend([y0, y1, None])
        edge_trace_mt = go.Scatter(
            x=edge_x_mt,
            y=edge_y_mt,
            mode='lines',
            legendgroup='Linhas MT',
            name='Linhas MT',
            line=dict(width=1, color='blue'),
            hoverinfo='none',
            showlegend=True)
        edge_trace_bt = go.Scatter(
            x=edge_x_bt,
            y=edge_y_bt,
            mode='lines',
            legendgroup='Linhas BT',
            name='Linhas BT',
            line=dict(width=0.8, color='green',dash='dash'),
            hoverinfo='none',
            showlegend=True)
        
        mid_x_mt = []
        mid_y_mt = []
        edge_text_mt = []
        mid_x_bt = []
        mid_y_bt = []
        edge_text_bt = []

        for u, v, data in subgrafo.edges(data=True):
            if u in pos and v in pos:
                x0, y0 = pos[u]
                x1, y1 = pos[v]
                if data['tipo'] == 'SSDBT' or data['tipo'] == 'RAMLIG':
                    mid_x_bt.append((x0 + x1) / 2)
                    mid_y_bt.append((y0 + y1) / 2)
                    edge_text_bt.append(
                        f"COD_ID: {data['cod_id']}<br>"
                        f"Elem: {data['tipo']}<br>"
                        f"PAC1:{u}<br>"
                        f"PAC2:{v}")
                else: 
                    mid_x_mt.append((x0 + x1) / 2)
                    mid_y_mt.append((y0 + y1) / 2)
                    edge_text_mt.append(
                        f"COD_ID: {data['cod_id']}<br>"
                        f"Elem: {data['tipo']}<br>"
                        f"PAC1:{u}<br>"
                        f"PAC2:{v}")
                
        edge_hover_trace_mt = go.Scatter(
        x=mid_x_mt,
        y=mid_y_mt,
        mode='markers',
        marker=dict(
            size=10,
            opacity=0 #invisível
        ),
        text=edge_text_mt,
        hoverinfo='text',
        hoverlabel=dict(
        bgcolor="lightblue",
        bordercolor="black",
        font_color="black"),
        legendgroup='Linhas MT',
        showlegend=False)

        edge_hover_trace_bt = go.Scatter(
        x=mid_x_bt,
        y=mid_y_bt,
        mode='markers',
        marker=dict(
            size=10,
            opacity=0 #invisível
        ),
        text=edge_text_bt,
        hoverinfo='text',
        hoverlabel=dict(
        bgcolor="green",
        bordercolor="black",
        font_color="black"),
        legendgroup='Linhas BT',
        showlegend=False)
        # Coordenadas dos nós
        node_x_bt = []
        node_y_bt = []
        node_text_bt = []
        node_x_mt = []
        node_y_mt = []
        node_text_mt = []
        node_x_ucbt = []
        node_y_ucbt = []
        node_text_ucbt = []
        node_x_tr = []
        node_y_tr = []
        node_text_tr = []
        node_x_ucmt = []
        node_y_ucmt = []
        node_text_ucmt = []
        for node,data in subgrafo.nodes(data=True):
            x, y = pos[node]
            if data['tipo'] == 'MT':
                node_x_mt.append(x)
                node_y_mt.append(y)
                node_text_mt.append(node)
            elif data['tipo'] == 'BT':
                node_x_bt.append(x)
                node_y_bt.append(y)
                node_text_bt.append(node)
            elif data['tipo'] == 'UCBT':
                node_x_ucbt.append(x)
                node_y_ucbt.append(y)
                node_text_ucbt.append(f"COD_ID:{data['cod_id']}<br>"
                    f"Elem:{data['tipo']}<br>"
                    f"PAC:{node}<br>")
            elif data['tipo'] == 'UNTRMT':
                node_x_tr.append(x)
                node_y_tr.append(y)
                node_text_tr.append(f"COD_ID:{data['cod_id']}<br>"
                    f"Elem:{data['tipo']}<br>"
                    f"PAC:{node}<br>")
            else:
                node_x_ucmt.append(x)
                node_y_ucmt.append(y)
                node_text_ucmt.append(f"COD_ID:{data['cod_id']}<br>"
                    f"Elem:{data['tipo']}<br>"
                    f"PAC:{node}<br>")

        node_trace_mt = go.Scatter(
            x=node_x_mt,
            y=node_y_mt,
            mode='markers',
            hoverinfo='text',
            text=node_text_mt,
            legendgroup='barras MT',
            name='barras MT',
            marker=dict(
                size=2,
                color='blue'
            ))
        node_trace_bt = go.Scatter(
            x=node_x_bt,
            y=node_y_bt,
            mode='markers',
            hoverinfo='text',
            text=node_text_bt,
            legendgroup='barras BT',
            name='barras BT',
            marker=dict(
                size=2,
                color='green'
            ))
        node_trace_ucbt = go.Scatter(
            x=node_x_ucbt,
            y=node_y_ucbt,
            mode='markers',
            hoverinfo='text',
            text=node_text_ucbt,
            legendgroup='UCBT',
            name='UCBT',
            marker=dict(
                size=3,
                color='forestgreen',
                symbol='diamond'
            ))
        node_trace_ucmt = go.Scatter(
            x=node_x_ucmt,
            y=node_y_ucmt,
            mode='markers',
            hoverinfo='text',
            text=node_text_ucmt,
            legendgroup='UCMT',
            name='UCMT',
            marker=dict(
                size=4,
                color='blue',
                symbol='square'
            ))
        node_trace_tr = go.Scatter(
            x=node_x_tr,
            y=node_y_tr,
            mode='markers',
            hoverinfo='text',
            text=node_text_tr,
            legendgroup='UNTRMT',
            name='UNTRMT',
            marker=dict(
                symbol='triangle-down',
                size=5,
                color='purple'
            ))
        return [edge_trace_mt, edge_trace_bt, edge_hover_trace_mt, edge_hover_trace_bt, node_trace_bt, node_trace_mt,node_trace_ucbt,node_trace_ucmt,node_trace_tr]

    def iso_points(self,df_isolated,pos):
        grafo = nx.Graph()
        for row in df_isolated.itertuples():
            p1 = _pac_key(row.PAC_1)
            p2 = _pac_key(row.PAC_2)
            if p1 is None or p2 is None:
                continue
            if row.ELEM == 'UCBT' or row.ELEM == 'UCMT':
                grafo.add_node(p1,cod_id=row.COD_ID,tipo=row.ELEM)
            elif row.ELEM == 'SSDBT' or row.ELEM == 'RAMLIG':
                grafo.add_node(p1)
                grafo.add_node(p2)
                grafo.add_edge(p1, p2, cod_id=row.COD_ID,tipo=row.ELEM)
            else:
                grafo.add_node(p1)
                if row.ELEM == 'UNTRMT':
                    grafo.add_node(p2,cod_id=row.COD_ID,tipo='UNTRMT')#TODO continuar
                else:
                    grafo.add_node(p2)
                grafo.add_edge(p1, p2, cod_id=row.COD_ID,tipo=row.ELEM)
        try:
            grafo.remove_node('')
        except:
            pass
        valid_nodes = [n for n in grafo.nodes if n in pos]
        subgrafo = grafo.subgraph(valid_nodes)
        edge_x_mt = []
        edge_y_mt = []
        edge_x_bt = []
        edge_y_bt = []

        for u, v, data in subgrafo.edges(data=True):
            x0, y0 = pos[u]
            x1, y1 = pos[v]
            if data['tipo'] == 'SSDBT' or data['tipo'] == 'RAMLIG':
                edge_x_bt.extend([x0, x1, None])
                edge_y_bt.extend([y0, y1, None])
            else:
                edge_x_mt.extend([x0, x1, None])
                edge_y_mt.extend([y0, y1, None])
        edge_trace_mt = go.Scatter(
            x=edge_x_mt,
            y=edge_y_mt,
            mode='lines',
            legendgroup='Linhas MT isoladas',
            name='Linhas MT isoladas',
            line=dict(width=1, color='red'),
            hoverinfo='none',
            showlegend=True)
        edge_trace_bt = go.Scatter(
            x=edge_x_bt,
            y=edge_y_bt,
            mode='lines',
            legendgroup='Linhas BT isoladas',
            name='Linhas BT isoladas',
            line=dict(width=0.8, color='red'),
            hoverinfo='none',
            showlegend=True)
        
        mid_x_mt = []
        mid_y_mt = []
        edge_text_mt = []
        mid_x_bt = []
        mid_y_bt = []
        edge_text_bt = []

        for u, v, data in subgrafo.edges(data=True):
            if u in pos and v in pos:
                x0, y0 = pos[u]
                x1, y1 = pos[v]
                if data['tipo'] == 'SSDBT' or data['tipo'] == 'RAMLIG':
                    mid_x_bt.append((x0 + x1) / 2)
                    mid_y_bt.append((y0 + y1) / 2)
                    edge_text_bt.append(
                        f"COD_ID: {data['cod_id']}<br>"
                        f"Elem: {data['tipo']}<br>"
                        f"PAC1:{u}<br>"
                        f"PAC2:{v}")
                else: 
                    mid_x_mt.append((x0 + x1) / 2)
                    mid_y_mt.append((y0 + y1) / 2)
                    edge_text_mt.append(
                        f"COD_ID: {data['cod_id']}<br>"
                        f"Elem: {data['tipo']}<br>"
                        f"PAC1:{u}<br>"
                        f"PAC2:{v}")          
        edge_hover_trace_mt = go.Scatter(
        x=mid_x_mt,
        y=mid_y_mt,
        mode='markers',
        marker=dict(
            size=10,
            opacity=0 #invisível
        ),
        text=edge_text_mt,
        hoverinfo='text',
        hoverlabel=dict(
        bgcolor="red",
        bordercolor="black",
        font_color="black"),
        legendgroup='Linhas MT isoladas',
        showlegend=False)

        edge_hover_trace_bt = go.Scatter(
        x=mid_x_bt,
        y=mid_y_bt,
        mode='markers',
        marker=dict(
            size=10,
            opacity=0 #invisível
        ),
        text=edge_text_bt,
        hoverinfo='text',
        hoverlabel=dict(
        bgcolor="red",
        bordercolor="black",
        font_color="black"),
        legendgroup='Linhas BT isoladas',
        showlegend=False)
        # Coordenadas dos nós
        node_x_iso = []
        node_y_iso = []
        node_text_iso = []
        node_x_ucbt = []
        node_y_ucbt = []
        node_text_ucbt = []
        node_x_tr = []
        node_y_tr = []
        node_text_tr = []
        node_x_ucmt = []
        node_y_ucmt = []
        node_text_ucmt = []
        for node,data in subgrafo.nodes(data=True):
            x, y = pos[node]
            tipo = data.get('tipo','')
            try:
                if tipo == 'UCBT':
                    node_x_ucbt.append(x)
                    node_y_ucbt.append(y)
                    node_text_ucbt.append(f"COD_ID:{data['cod_id']}<br>"
                        f"Elem:{data['tipo']}<br>"
                        f"PAC:{node}<br>")
                elif tipo == 'UNTRMT':
                    node_x_tr.append(x)
                    node_y_tr.append(y)
                    node_text_tr.append(f"COD_ID:{data['cod_id']}<br>"
                        f"Elem:{data['tipo']}<br>"
                        f"PAC:{node}<br>")
                elif tipo == 'UCMT':
                    node_x_ucmt.append(x)
                    node_y_ucmt.append(y)
                    node_text_ucmt.append(f"COD_ID:{data['cod_id']}<br>"
                        f"Elem:{data['tipo']}<br>"
                        f"PAC:{node}<br>")
                else:
                    node_x_iso.append(x)
                    node_y_iso.append(y)
                    node_text_iso.append(node)
            except:
                print('z')

        node_trace_iso = go.Scatter(
            x=node_x_iso,
            y=node_y_iso,
            mode='markers',
            hoverinfo='text',
            text=node_text_iso,
            legendgroup='PACs isolados',
            name='PACs isolados',
            marker=dict(
                size=4,
                color='red',
                symbol='circle'
            ))
        node_trace_ucbt = go.Scatter(
            x=node_x_ucbt,
            y=node_y_ucbt,
            mode='markers',
            hoverinfo='text',
            text=node_text_ucbt,
            legendgroup='UCBT isolada',
            name='UCBT isolada',
            marker=dict(
                size=5,
                color='red',
                symbol='diamond'
            ))
        node_trace_ucmt = go.Scatter(
            x=node_x_ucmt,
            y=node_y_ucmt,
            mode='markers',
            hoverinfo='text',
            text=node_text_ucmt,
            legendgroup='UCMT isolada',
            name='UCMT isolada',
            marker=dict(
                size=6,
                color='red',
                symbol='square'
            ))
        node_trace_tr = go.Scatter(
            x=node_x_tr,
            y=node_y_tr,
            mode='markers',
            hoverinfo='text',
            text=node_text_tr,
            legendgroup='UNTRMT isolado',
            name='UNTRMT isolado',
            marker=dict(
                symbol='triangle-down',
                size=5,
                color='red'
            ))
        return [edge_trace_mt,edge_hover_trace_mt,edge_trace_bt,edge_hover_trace_bt,node_trace_iso,node_trace_ucbt,node_trace_ucmt,node_trace_tr]
    
    def plot_fase_error(self,df_mt,df_bt,fase_error,fase_error_bt,pac_ctmt,feeder,path_coords,output_path):
        """
        df_mt -> dataframe dos elementos de média tensão
        df_bt -> dataframe dos elementos de baixa tensão
        fase_error -> erros de faseamento em média tensão
        fase_error_bt -> erros de faseamento em baixa tensão
        pac_ctmt -> ponto de acoplamento inicial do alimentaodr
        feeder -> nome do alimentador
        path_coords -> caminho do arquivo de coordenadas em csv
        output_path -> caminho de salvamento da figura de saída"""
        df = pd.read_csv(path_coords)
        pos = {_pac_key(row["PAC"]): (row["long"], row["lat"])
            for _, row in df.iterrows()
            if _pac_key(row["PAC"]) is not None}
        df_trafo = df_mt[df_mt['ELEM'] == 'EQTRMT']
        traces_mt = self.graph_points_fase_mt(df_mt=df_mt,fase_error=fase_error,pac_ctmt=pac_ctmt,feeder=feeder,pos=pos)
        traces_bt = self.graph_points_fase_bt(df_bt=df_bt,df_trafo=df_trafo,fase_error=fase_error_bt,pos=pos)
        fig = go.Figure(
        data = traces_mt + traces_bt)
        fig.add_annotation(
            x=0,
            y=1.05,
            xref="paper",
            yref="paper",
            text=f"Alimentador:{feeder} - Erros de Faseamento",
            showarrow=False)
        fig_path = os.path.join(output_path, f"figuras_{feeder}")
        
        if not os.path.exists(fig_path):
            os.makedirs(fig_path)

        path = os.path.join(fig_path,f"fase_error_{feeder}.html")

        fig.write_html(path)

    def graph_points_fase_mt(self,df_mt,fase_error,pac_ctmt,feeder,pos):
        grafo = nx.Graph()
        for row in df_mt.itertuples(index=False):
            p1 = _pac_key(row.PAC_1)
            p2 = _pac_key(row.PAC_2)
            if row.ELEM == 'UCMT':
                if p1 is None:
                    continue
                grafo.add_node(p1,cod_id=row.COD_ID,fas_con=row.FAS_CON,tipo=row.ELEM)
            else:
                if p1 is None or p2 is None:
                    continue
                grafo.add_node(p1, tipo='bus')
                grafo.add_node(p2, tipo='bus')
                grafo.add_edge(p1, p2, cod_id=row.COD_ID,fas_con=row.FAS_CON,tipo=row.ELEM)
        try:
            grafo.remove_node('')
        except:
            pass
        ##TODO agora plotar e mostrar 
        valid_nodes = [n for n in grafo.nodes if n in pos]
        subgrafo = grafo.subgraph(valid_nodes)
        # Coordenadas das linhas
        edge_x_normal = []
        edge_y_normal = []
        edge_x_error = []
        edge_y_error = []
        for u, v, data in subgrafo.edges(data=True):

            x0, y0 = pos[u]
            x1, y1 = pos[v]

            if str(data['cod_id']).lower() in fase_error:
                edge_x_error.extend([x0, x1, None])
                edge_y_error.extend([y0, y1, None])
            else:
                edge_x_normal.extend([x0, x1, None])
                edge_y_normal.extend([y0, y1, None])

        edge_trace = go.Scatter(
            x=edge_x_normal,
            y=edge_y_normal,
            mode='lines',
            line=dict(width=1, color='steelblue'),
            hoverinfo='none',
            name='Linhas MT')
        
        edge_error_trace = go.Scatter(
            x=edge_x_error,
            y=edge_y_error,
            mode='lines',
            line=dict(width=4, color='red'),
            hoverinfo='none',
            name='Faseamento incorreto MT')

        mid_x = []
        mid_y = []
        edge_text = []

        mid_x_error = []
        mid_y_error = []
        edge_text_error = []

        for u, v, data in grafo.edges(data=True):
            if u in pos and v in pos:
                x0, y0 = pos[u]
                x1, y1 = pos[v]
                x_mid = (x0 + x1)/2
                y_mid = (y0 + y1)/2
            else:
                continue

            if str(data['cod_id']).lower() in fase_error.keys():
                mid_x_error.append(x_mid)
                mid_y_error.append(y_mid)
                edge_text_error.append(
                    f"Entidade: {data['tipo']}<br>"
                    f"COD_ID: {data['cod_id']}<br>"
                    f"FAS_CON: {fase_error[data['cod_id']]}<br>"
                    f"PAC1:{u}<br>"
                    f"PAC2:{v}")
            else:
                mid_x.append(x_mid)
                mid_y.append(y_mid)
                edge_text.append(
                    f"Entidade: {data['tipo']}<br>"
                    f"COD_ID: {data['cod_id']}<br>"
                    f"FAS_CON: {data['fas_con']}<br>"
                    f"PAC1:{u}<br>"
                    f"PAC2:{v}")

        edge_hover_trace = go.Scatter(
        x=mid_x,
        y=mid_y,
        mode='markers',
        marker=dict(
            size=10,
            opacity=0  # invisível
        ),
        text=edge_text,
        hoverinfo='text',
        hoverlabel=dict(
        bgcolor="lightblue",
        bordercolor="black",
        font_color="black"),
        showlegend=False)

        edge_hover_trace_error = go.Scatter(
        x=mid_x_error,
        y=mid_y_error,
        mode='markers',
        marker=dict(size=10, opacity=0),
        text=edge_text_error,
        hoverinfo='text',
        hoverlabel=dict(
            bgcolor='salmon',      # ou 'lightcoral'
            bordercolor='red',
            font_color='black'
        ),
        showlegend=False)
        # Coordenadas dos nós e UCMTs
        node_x = []
        node_y = []
        node_text = []
        node_x_ucmt = []
        node_y_ucmt = []
        node_text_ucmt = []
        node_color_ucmt = []
        for node,data in subgrafo.nodes(data=True):
            x, y = pos[node]
            if data['tipo'] == 'UCMT':
                if str(data['cod_id']).lower() in fase_error:
                    node_color_ucmt.append('red')
                else:
                    node_color_ucmt.append('royalblue')
                node_x_ucmt.append(x)
                node_y_ucmt.append(y)
                node_text_ucmt.append(
                    f"Entidade: {data['tipo']}<br>"
                    f"COD_ID: {data['cod_id']}<br>"
                    f"FAS_CON: {data['fas_con']}<br>"
                    f"PAC:{node}")
            else:
                node_x.append(x)
                node_y.append(y)
                node_text.append(node)
        #TODO criar dois node_trace -> node_trace_ucmt/node_trace
        node_trace = go.Scatter(
            x=node_x,
            y=node_y,
            mode='markers',
            hoverinfo='text',
            text=node_text,
            name='Barras MT',
            marker=dict(size=3.5,color='blue'))
        
        node_trace_ucmt = go.Scatter(
            x=node_x_ucmt,
            y=node_y_ucmt,
            mode='markers',
            text=node_text_ucmt,
            hoverinfo='text',
            name='UCMT',
            marker=dict(
                size=8,
                color=node_color_ucmt,
                symbol='square'))
        
        highlight_trace = None
        pac_ctmt_key = _pac_key(pac_ctmt)
        if pac_ctmt_key in pos:
            x, y = pos[pac_ctmt_key]
            highlight_trace = go.Scatter(
                x=[x],
                y=[y],
                mode='markers',
                marker=dict(
                    size=15,
                    color='black',
                    symbol='triangle-up'),
                text=[f'PAC:{pac_ctmt}<br>'
                      f'Alimentador:{feeder}'],
                hoverinfo='text',
                name=f'SE')
            
        fig = go.Figure(
        data=[
            edge_trace,
            edge_error_trace,
            edge_hover_trace,
            edge_hover_trace_error,
            node_trace,
            node_trace_ucmt,
            highlight_trace
        ])
        return [edge_trace,edge_error_trace,edge_hover_trace,edge_hover_trace_error, node_trace, node_trace_ucmt, highlight_trace]
    
    def graph_points_fase_bt(self,df_bt,df_trafo,fase_error,pos): #TODO fazer funcionar essa rotina
        grafos, pacs_trf = self.return_graph_trafo_plot(df_bt=df_bt,df_trafo=df_trafo)
        traces = []
        show_legend_tr = True
        show_legend_error = True
        show_legend_node = True
        show_legend_node_ucbt = True
        show_legend_ssdbt = True
        ##TODO agora plotar e mostrar 
        for grafo,pac_trf in zip(grafos,pacs_trf):
            valid_nodes = [n for n in grafo.nodes if _pac_key(n) in pos]
            subgrafo = grafo.subgraph(valid_nodes)
            if len(subgrafo.nodes) == 0:
                continue
            # Coordenadas das linhas
            edge_x_normal = []
            edge_y_normal = []
            edge_x_error = []
            edge_y_error = []
            for u, v, data in subgrafo.edges(data=True):
                u_key = _pac_key(u)
                v_key = _pac_key(v)
                if u_key is None or v_key is None:
                    continue

                x0, y0 = pos[u_key]
                x1, y1 = pos[v_key]

                if str(data['cod_id']).lower() in fase_error:
                    edge_x_error.extend([x0, x1, None])
                    edge_y_error.extend([y0, y1, None])
                else:
                    edge_x_normal.extend([x0, x1, None])
                    edge_y_normal.extend([y0, y1, None])

            edge_trace = go.Scatter(
                x=edge_x_normal,
                y=edge_y_normal,
                mode='lines',
                line=dict(width=0.5, color='forestgreen'),
                hoverinfo='none',
                legendgroup = 'Linhas BT',
                showlegend=show_legend_ssdbt,
                name='Linhas BT')

            edge_error_trace = go.Scatter(
                x=edge_x_error,
                y=edge_y_error,
                mode='lines',
                line=dict(width=4, color='lightcoral'),
                hoverinfo='none',
                legendgroup= 'Faseamento incorreto BT',
                showlegend=show_legend_error,
                name='Faseamento incorreto BT')
            if len(edge_x_error) > 0: #para aparecer apenas na primeira vez a legenda
                show_legend_error = False
            if len(edge_x_normal) > 0:#para aparecer apenas na primeira vez a legenda
                show_legend_ssdbt = False
            mid_x = []
            mid_y = []
            edge_text = []

            mid_x_error = []
            mid_y_error = []
            edge_text_error = []

            for u, v, data in grafo.edges(data=True):
                u_key = _pac_key(u)
                v_key = _pac_key(v)
                if u_key in pos and v_key in pos:
                    x0, y0 = pos[u_key]
                    x1, y1 = pos[v_key]
                    x_mid = (x0 + x1)/2
                    y_mid = (y0 + y1)/2
                    if str(data['cod_id']).lower() in fase_error.keys():
                        mid_x_error.append(x_mid)
                        mid_y_error.append(y_mid)
                        edge_text_error.append(
                        f"Entidade: {data['tipo']}<br>"
                        f"COD_ID: {data['cod_id']}<br>"
                        f"FAS_CON: {fase_error[data['cod_id']]}<br>"
                        f"PAC1:{u}<br>"
                        f"PAC2:{v}")
                    else:
                        mid_x.append(x_mid)
                        mid_y.append(y_mid)
                        edge_text.append(
                        f"Entidade: {data['tipo']}<br>"
                        f"COD_ID: {data['cod_id']}<br>"
                        f"FAS_CON: {data['fas_con']}<br>"
                        f"PAC1:{u}<br>"
                        f"PAC2:{v}")

            edge_hover_trace = go.Scatter(
            x=mid_x,
            y=mid_y,
            mode='markers',
            marker=dict(
                size=10,
                opacity=0  # invisível
            ),
            text=edge_text,
            hoverinfo='text',
            hoverlabel=dict(
            bgcolor="lightblue",
            bordercolor="black",
            font_color="black"),
            showlegend=False)

            edge_hover_trace_error = go.Scatter(
            x=mid_x_error,
            y=mid_y_error,
            mode='markers',
            marker=dict(size=10, opacity=0),
            text=edge_text_error,
            hoverinfo='text',
            hoverlabel=dict(
                bgcolor='salmon',      # ou 'lightcoral'
                bordercolor='red',
                font_color='black'
            ),
            showlegend=False)
            # Coordenadas dos nós
            node_x = []
            node_y = []
            node_text = []
            node_x_ucbt = []
            node_y_ucbt = []
            node_text_ucbt = []
            node_color_ucbt = []

            for node,data in subgrafo.nodes(data=True):
                node_key = _pac_key(node)
                if node_key is None:
                    continue
                x, y = pos[node_key]
                if len(data) > 0 and data['tipo'] == 'UCBT':
                    if str(data['cod_id']).lower() in fase_error:
                        node_color_ucbt.append('darkorange')
                    else:
                        node_color_ucbt.append('limegreen')
                    node_x_ucbt.append(x)
                    node_y_ucbt.append(y)
                    node_text_ucbt.append(
                        f"Entidade: {data['tipo']}<br>"
                        f"COD_ID: {data['cod_id']}<br>"
                        f"FAS_CON: {data['fas_con']}<br>"
                        f"PAC:{node}")
                else:
                    node_x.append(x)
                    node_y.append(y)
                    node_text.append(node)
            ##TODO criar dois node_trace -> node_trace_ucbt/node_trace
            node_trace = go.Scatter(
                x=node_x,
                y=node_y,
                mode='markers',
                hoverinfo='text',
                text=node_text,
                legendgroup='Barras BT',
                name='Barras BT',
                showlegend=show_legend_node,
                marker=dict(size=3.5,color='blue'))
            
            node_trace_ucbt = go.Scatter(
                x=node_x_ucbt,
                y=node_y_ucbt,
                mode='markers',
                text=node_text_ucbt,
                hoverinfo='text',
                legendgroup='UCBT',
                name='UCBT',
                showlegend=show_legend_node_ucbt,
                marker=dict(
                    size=4.5,
                    color=node_color_ucbt,
                    symbol='square'))
            
            if len(node_text_ucbt) > 0:
                show_legend_node_ucbt = False
            
            if len(node_text) > 0:
                show_legend_node = False
                
            traces.extend([edge_trace,edge_error_trace,edge_hover_trace,edge_hover_trace_error,node_trace,node_trace_ucbt])
            text_trf = []
            pac_trf_key = _pac_key(pac_trf)
            if pac_trf_key in pos:
                x, y = pos[pac_trf_key]
                i = df_trafo[df_trafo['PAC_2'] == pac_trf].index[0]
                text_trf.append(
                        f"Entidade:UNTRMT/EQTRMT<br>"
                        f"COD_ID: {df_trafo.at[i,'COD_ID']}<br>"
                        f"pac1:{df_trafo.at[i, 'PAC_1']}➔pac2:{pac_trf}<br>"
                        f"LIG_FAS_P: {df_trafo.at[i, 'FAS_CON']}<br>"
                        f"LIG_FAS_S: {df_trafo.at[i, 'LIG_FAS_S']}<br>"
                        f"LIG_FAS_T: {df_trafo.at[i, 'LIG_FAS_T']}<br>")
                highlight_trace_tr = go.Scatter(
                    x=[x],
                    y=[y],
                    mode='markers',
                    marker=dict(
                        size=8,
                        color='indigo',
                        symbol='triangle-down'
                    ),
                    text=text_trf,
                    hoverinfo='text',
                    legendgroup='TRs',
                    showlegend=show_legend_tr,
                    name='TR'
                )
                highlight_trace_tr.legendgroup = 'TR'
                traces.append(highlight_trace_tr)
                show_legend_tr = False

        return traces
    
    def return_graph_trafo_plot(self,df_bt,df_trafo):
        """Retorna o grafo em sequencia do transformador BT escolhido"""
        pacs_trfs = []
        grafos = []
        for trafo in df_trafo['COD_ID'].tolist():
            if trafo == None:
                #print('trafo sem COD_ID')
                continue
            transformador = trafo
            pac_trafo = df_trafo.at[df_trafo[df_trafo['COD_ID'] == trafo].index[0],'PAC_2']
            pacs_trfs.append(pac_trafo)
            #TODO não esta aparecendo as cargas
            df_line_bt = df_bt.query("UNI_TR_MT == @transformador")
            grafo = nx.Graph()
            for row in df_line_bt.itertuples(index=False):
                if row.ELEM == 'UCBT':
                    grafo.add_node(row.PAC_1,cod_id=row.COD_ID, fas_con=row.FAS_CON, tipo='UCBT')
                else:
                    grafo.add_node(row.PAC_1, tipo='bus')
                    grafo.add_node(row.PAC_2)
                    grafo.add_edge(row.PAC_1, row.PAC_2, cod_id=row.COD_ID, fas_con=row.FAS_CON, tipo=row.ELEM)
            
            grafos.append(grafo)
        
        return grafos, pacs_trfs
    
        
