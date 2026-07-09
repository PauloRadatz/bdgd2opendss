# -*- coding: utf-8 -*-
# @Author  : Paulo Radatz
# @Email   : paulo.radatz@gmail.com
# @File    : Settings.py
# @Software: PyCharm

import locale
from dataclasses import dataclass, field


def _detect_csv_separator() -> str:
    current_locale = locale.setlocale(locale.LC_NUMERIC)
    try:
        locale.setlocale(locale.LC_NUMERIC, "")
        decimal_point = locale.localeconv().get("decimal_point", ".")
    except locale.Error:
        decimal_point = "."
    finally:
        locale.setlocale(locale.LC_NUMERIC, current_locale)
    return ";" if decimal_point == "," else ","

@dataclass
class Settings:
    limitRamal30m: bool = field(default=True, metadata={"description": "Limits ramal to 30m"})
    ger4fios: bool = field(default=True, metadata={"description": "Generates with Neutral"})
    gerCapacitors: bool = field(default=False, metadata={"description": "Generates capacitor banks"})
    loadModel: str = field(default="ANEEL", metadata={"description": "Load model (ANEEL or model8)"})
    genTypeMT: str = field(default="asBDGD", metadata={"description": "MT generator type: generator/PVSystem/asBDGD"})
    genTypeBT: str = field(default="generator", metadata={"description": "BT generator type: generator/PVSystem"})
    gerCoord: bool = field(default=True, metadata={"description": "Controls geographic generation"})

    # TODO options in the ProgGeoPerdas
    intRealizaCnvrgcPNT: bool = field(default=False, metadata={"description": "Convergência de Perda Não Técnica"})
    intUsaTrafoABNT: bool = field(default=False, metadata={"description": "Transformadores ABNT"})
    intAdequarTensaoCargasMT: bool = field(default=False, metadata={"description": "Adequação de Tensão Mínima das Cargas MT (0.93 pu)"})
    intAdequarTensaoCargasBT: bool = field(default=False, metadata={"description": "Adequação de Tensão Mínima das Cargas BT (0.92 pu)"})
    intAdequarTensaoSuperior: bool = field(default=False, metadata={"description": "Limitar Máxima Tensão de Barras e Reguladores (1.05 pu)"})
    intAdequarRamal: bool = field(default=False, metadata={"description": "Limitar o Ramal (30m)"})
    intAdequarModeloCarga: int = field(default=1, metadata={"description": "Adequar Modelo de Carga"})
    intAdequarTapTrafo: bool = field(default=False, metadata={"description": "Utilizar Tap nos Transformadores"})
    intAdequarPotenciaCarga: bool = field(default=False, metadata={"description": "Limitar Cargas BT (Potência ativa do transformador)"})
    intAdequarTrafoVazio: bool = field(default=False, metadata={"description": "Eliminar Transformadores Vazios"})
    intNeutralizarTrafoTerceiros: bool = field(default=False, metadata={"description": "Neutralizar Transformadores de Terceiros"})
    intNeutralizarRedeTerceiros: bool = field(default=False, metadata={"description": "Neutralizar Redes de Terceiros (MT/BT)"})
    intModeloConverge: bool = field(default=False, metadata={"description": "Modelo de Convergência"})
    dblVPUMin: float = field(default=0.5, metadata={"description": "Tensão Mínima(pu)"})
    cbMeterComplete: bool = field(default=False, metadata={"description": "Medidores de Energia no barramento principal e transformadores"})
    # BDGD pública ou PRIVADA
    TipoBDGD: bool = field(default=False, metadata={"description": "Define o arquivo JSON para a BDGD: privada (True) ou pública (False)"})
    TabelaPT: bool = field(default=True, metadata={"description": "Define se o usuário quer gerar as tabelas de perdas técnicas"})
    csv_separator: str = field(default_factory=_detect_csv_separator, metadata={"description": "Separador usado nos arquivos CSV"})

settings = Settings()
