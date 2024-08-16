# -*- encoding: utf-8 -*-
"""
 * Project Name: main.py
 * Created by migueldcga
 * Date: 25/10/2023
 * Time: 18:55
 *
 * Edited by:
 * Date:
 * Time:
"""
# Não remover a linha de importação abaixo
import copy
import re
from typing import Any
import geopandas as gpd
from tqdm import tqdm

from bdgd2opendss.model.Converter import convert_tten
from bdgd2opendss.core.Utils import create_output_file

from dataclasses import dataclass


@dataclass
class LineCode:
    _basefreq: float = 60
    _units: str = "km"
    _linecode: str = ""
    _nphases: int = 4
    _normamps: float = 520.00
    _r1: float = 0.0000
    _x1: float = 0.0001

    _linecode_1: str = ""
    _linecode_2: str = ""
    _linecode_3: str = ""
    _linecode_4: str = ""

    @property
    def basefreq(self):
        return self._basefreq

    @basefreq.setter
    def basefreq(self, value: float):
        self._basefreq = value

    @property
    def units(self):
        return self._units

    @units.setter
    def units(self, value: str):
        self._units = value

    @property
    def linecode(self):
        return self._linecode

    @linecode.setter
    def linecode(self, value: str):
        self._linecode = value

    @property
    def nphases(self):
        return self._nphases

    @nphases.setter
    def nphases(self, value: int):
        self._nphases = value

    @property
    def normamps(self):
        return self._normamps

    @normamps.setter
    def normamps(self, value: float):
        self._normamps = value

    @property
    def r1(self):
        return self._r1

    @r1.setter
    def r1(self, value: float):
        self._r1 = value

    @property
    def x1(self):
        return self._x1

    @x1.setter
    def x1(self, value: float):
        self._x1 = value

    @property
    def linecode_1(self):
        return self._linecode_1

    @linecode_1.setter
    def linecode_1(self, value: str):
        self._linecode_1 = value

    @property
    def linecode_2(self):
        return self._linecode_2

    @linecode_2.setter
    def linecode_2(self, value: str):
        self._linecode_2 = value

    @property
    def linecode_3(self):
        return self._linecode_3

    @linecode_3.setter
    def linecode_3(self, value: str):
        self._linecode_3 = value

    @property
    def linecode_4(self):
        return self._linecode_4

    @linecode_4.setter
    def linecode_4(self, value: str):
        self._linecode_4 = value

    def pattern_string(self) -> str:
        return f"New \"Linecode.{self.linecode}\" nphases={self.nphases} " \
            f"basefreq={self.basefreq} r1={self.r1:.4f} x1={self.x1:.4f} " \
            f"units={self.units} normamps={self.normamps:.2f}"

    def full_string(self) -> str:
        return f"{self.linecode_1}\n" \
               f"{self.linecode_2}\n" \
               f"{self.linecode_3}\n" \
               f"{self.linecode_4}\n" \

    def __repr__(self):
        return f"{self.linecode_1}\n" \
               f"{self.linecode_2}\n" \
               f"{self.linecode_3}\n" \
               f"{self.linecode_4}\n" \

    @staticmethod
    def rename_linecode_string(linecode_, i, input_str: str) -> str:
        """This function re-writes the string identfying key places by specified parameters and insering caracteres.

        Args:

        Returns:

        In this case, it should modify the names of line, bus1, bus2 and linecode.

        """
        pattern = r'New "Linecode.([^\s"]+)" nphases=(\d+)'

        def repl(match):
            linecode_num = match.group(1)
            nphases_value = match.group(2)
            return f'New "Linecode.{linecode_num}_{nphases_value}" nphases={nphases_value}'


        setattr(linecode_, f"_linecode_{i}", re.sub(pattern, repl, input_str))



    @staticmethod
    def _create_linecode_from_row(linecode_config, row):
        linecode_ = LineCode()

        for key, value in linecode_config.items():
            if key == "static":
                for static_key, static_value in value.items():
                    setattr(linecode_, f"_{static_key}", static_value)
            elif key == "direct_mapping":
                for mapping_key, mapping_value in value.items():
                    setattr(linecode_, f"_{mapping_key}", row[mapping_value])
            elif key == "indirect_mapping":
                for mapping_key, mapping_value in value.items():
                    if isinstance(mapping_value, list):
                        param_name, function_name = mapping_value
                        function_ = globals()[function_name]
                        param_value = row[param_name]
                        setattr(linecode_, f"_{mapping_key}", function_(param_value))
                    else:
                        setattr(linecode_, f"_{mapping_key}", row[mapping_value])

        return linecode_

    @staticmethod
    def create_linecode_from_json(json_data: Any, dataframe: gpd.geodataframe.GeoDataFrame, feeder: str):
        linecodes = []
        linecode_config = json_data['elements']['Linecode']['SEGCON']
        interactive = linecode_config.get('interactive')

        progress_bar = tqdm(dataframe.iterrows(), total=len(dataframe), desc="Linecode", unit=" linecodes", ncols=100)
        for _, row in progress_bar:
            linecode_ = LineCode._create_linecode_from_row(linecode_config, row)

            if interactive is not None: #parametro_iteravel, objeto
                for i in range(1, interactive['nphases'] + 1):
                    linecode_.nphases = i
                    LineCode.rename_linecode_string(linecode_, i, linecode_.pattern_string())
            linecodes.append(linecode_)

            progress_bar.set_description(f"Processing Linecode {_ + 1}")

        file_name = create_output_file(linecodes, linecode_config["arquivo"], feeder=feeder )

        return linecodes, file_name
