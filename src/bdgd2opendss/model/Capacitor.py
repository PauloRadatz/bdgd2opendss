# -*- encoding: utf-8 -*-
"""
 * Project Name: main.py
 * Created by anacmamede
 * Date: 13/04/2023
 * Time: 22:00
 *
 * Edited by: anacmamede
 * Date: 18/04/2023
 * Time: 21:35
"""
# Não remover a linha de importação abaixo
import copy
import re
from typing import Any
import geopandas as gpd
from tqdm import tqdm

from bdgd2opendss.model.Converter import convert_tpotrtv, convert_tfascon_phases, convert_tfascon_conn, convert_tfascon_bus #, convert_tgruten
# fazer função convert_tgruten

from dataclasses import dataclass

@dataclass
class Capacitor:
    _capacitor: str = ""
    _bus1: str = ""
    _kv: float = 0.0
    _kvar: float = 0.0
    _phases: int = 0
    _conn: str = ""
    _bus_nodes: str = ""

    @property
    def capacitor(self):
        return self._capacitor

    @capacitor.setter
    def capacitor(self, value: str):
        self._capacitor = value

    @property
    def bus1(self) -> str:
        return self._bus1

    @bus1.setter
    def bus1(self, value):
        self._bus1 = value

    @property
    def kv(self):
        return self._kv

    @kv.setter
    def kv(self, value: float):
        self._kv = value

    @property
    def kvar(self):
        return self._kvar

    @kvar.setter
    def kvar(self, value: float):
        self._kvar = value

    @property
    def phases(self):
        return self._phases

    @phases.setter
    def phases(self, value: int):
        self._phases = value

    @property
    def conn(self) -> str:
        return self._conn

    @conn.setter
    def conn(self, value):
        self._conn = value

    @property
    def bus_nodes(self) -> str:
        return self._bus_nodes

    @bus_nodes.setter
    def bus_nodes(self, value):
        self._bus_nodes = value


    def full_string(self) -> str:
        return f"New \"Capacitor.{self.capacitor}\" kv={self.kv} " \
               f"kvar={self.kvar} bus1=\"{self.bus1}\" phases={self.phases} " \
               f"conn={self.conn}"

    def __repr__(self):
        return f"New \"Capacitor.{self.capacitor}\" kv={self.kv} " \
               f"kvar={self.kvar} bus1=\"{self.bus1}\" phases={self.phases} " \
               f"conn={self.conn}"


    @staticmethod
    def _create_capacitor_from_row(capacitor_config, row):
        capacitor_ = Capacitor()

        for key, value in capacitor_config.items():
            if key == "static":
                for static_key, static_value in value.items():
                    setattr(capacitor_, f"_{static_key}", static_value)
            elif key == "direct_mapping":
                for mapping_key, mapping_value in value.items():
                    setattr(capacitor_, f"_{mapping_key}", row[mapping_value])
            elif key == "indirect_mapping":
                for mapping_key, mapping_value in value.items():
                    if isinstance(mapping_value, list):
                        param_name, function_name = mapping_value
                        function_ = globals()[function_name]
                        param_value = row[param_name]
                        setattr(capacitor_, f"_{mapping_key}", function_(param_value))
                    else:
                        setattr(capacitor_, f"_{mapping_key}", row[mapping_value])

        return capacitor_

    @staticmethod
    def create_capacitor_from_json(json_data: Any, dataframe: gpd.geodataframe.GeoDataFrame):
        capacitors = []
        capacitor_config = json_data['elements']['Capacitor']['UNCRMT']

        progress_bar = tqdm(dataframe.iterrows(), total=len(dataframe), desc="capacitor", unit=" capacitors", ncols=100)
        for _, row in progress_bar:
            capacitor_ = capacitor._create_capacitor_from_row(capacitor_config, row)

            capacitors.append(capacitor_)
            progress_bar.set_description(f"Processing Capacitor {_ + 1}")

        return capacitors
