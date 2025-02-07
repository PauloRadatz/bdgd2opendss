# -*- encoding: utf-8 -*-
"""
 * Project Name: main.py
 * Created by migueldcga
 * Date: 30/10/2023
 * Time: 23:53
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

from bdgd2opendss.model.Converter import convert_tfascon_phases, convert_tfascon_bus, convert_tfascon_quant_fios
from bdgd2opendss.core.Utils import create_output_file, ordem_pacs, elem_isolados
from bdgd2opendss.core.Settings import settings
from bdgd2opendss.model.Transformer import Transformer

from dataclasses import dataclass


@dataclass
class Line:

    _feeder: str = ""
    _units: str = "km"
    _bus1: str = ""
    _bus2: str = ""
    _bus_nodes: str = ""
    _line: str = ""
    _linecode: str = ""
    _suffix_linecode: str = ""
    _phases: int = 0
    _length: float = 0.0
    _prefix_name: str = ""
    _transformer: str = ""
    _estado: str = ""
    _posse: str = ""

    _entity: str =''

    _c0: float = 0.0
    _c1: float = 0.0
    _r0: float = 0.001
    _r1: float = 0.001
    _switch: str = "T"
    _x0: float = 0.0
    _x1: float = 0.0

    @property
    def entity(self):
        return self._entity

    @entity.setter
    def entity(self, value: str):
        self._entity = value

    @property
    def feeder(self):
        return self._feeder

    @feeder.setter
    def feeder(self, value: str):
        self._feeder = value

    @property
    def units(self):
        return self._units

    @units.setter
    def units(self, value: str):
        self._units = value

    @property
    def bus1(self):
        return self._bus1

    @bus1.setter
    def bus1(self, value: str):
        self._bus1 = value

    @property
    def bus2(self):
        return self._bus2

    @bus2.setter
    def bus2(self, value: str):
        self._bus2 = value

    @property
    def bus_nodes(self):
        return self._bus_nodes

    @bus_nodes.setter
    def bus_nodes(self, value: str):
        self._bus_nodes = value

    @property
    def line(self):
        return self._line

    @line.setter
    def line(self, value: str):
        self._line = value

    @property
    def linecode(self):
        return self._linecode

    @linecode.setter
    def linecode(self, value: str):
        self._linecode = value

    @property
    def suffix_linecode(self):
        return self._suffix_linecode

    @suffix_linecode.setter
    def suffix_linecode(self, value: str):
        self._suffix_linecode = value

    @property
    def phases(self):
        return self._phases

    @phases.setter
    def phases(self, value: int):
        self._phases = value

    @property
    def length(self):
        return self._length

    @length.setter
    def length(self, value: float):
        self._length = value

    @property
    def prefix_name(self):
        return self._prefix_name

    @prefix_name.setter
    def prefix_name(self, value: float):
        self._prefix_name = value

    @property
    def c0(self):
        return self._c0

    @c0.setter
    def c0(self, value: float):
        self._c0 = value

    @property
    def c1(self):
        return self._c1

    @c1.setter
    def c1(self, value: float):
        self._c1 = value

    @property
    def r0(self):
        return self._r0

    @r0.setter
    def r0(self, value: float):
        self._r0 = value

    @property
    def r1(self):
        return self._r1

    @r1.setter
    def r1(self, value: float):
        self._r1 = value

    @property
    def switch(self):
        return self._switch

    @switch.setter
    def switch(self, value: str):
        self._switch = value

    @property
    def x0(self):
        return self._x0

    @x0.setter
    def x0(self, value: float):
        self._x0 = value

    @property
    def x1(self):
        return self._x1

    @x1.setter
    def x1(self, value: float):
        self._x1 = value

    @property
    def transformer(self):
        return self._transformer

    @transformer.setter
    def transformer(self, value: str):
        self._transformer = value

    @property
    def estado(self):
        return self._estado

    @estado.setter
    def estado(self, value: str):
        self._estado = value

    @property
    def posse(self):
        return self._posse

    @posse.setter
    def posse(self, value: str):
        self._posse = value

    def neutraliza_rede_terceiros(self): #settings (neutraliza rede de terceiros)
        if settings.intNeutralizarRedeTerceiros:
            if (self.prefix_name == 'RBT' and self.transformer in Transformer.list_posse()) or (self.prefix_name != 'RBT' and self.posse != "PD"):
                linecode = 'r1=0.001 r0=0.001 x1=0 x0=0 c1=0 c0=0'
            else:
                linecode = f'linecode="{self.linecode}_{self.suffix_linecode}"'
        else:
            linecode = f'linecode="{self.linecode}_{self.suffix_linecode}"'
        return(linecode)
    
    def limitar_ramal(length): #settings (limitar ramal em 30 m)
        if length > 0.03 and settings.intAdequarRamal:
            new_lenght = 0.03
            return(new_lenght)
        else:
            return(length)
           
    def pattern_segment(self):

        linecode = Line.neutraliza_rede_terceiros(self)

        if self.prefix_name == "SMT": #TODO checar como fazer o sequenciamento dos buses
            if ordem_pacs() == 'Invertida': #define a ordem dos buses de acordo com o bus inicial
                self.bus2, self.bus1 = self.bus1, self.bus2 
            else:
                self.bus1, self.bus2 = self.bus1, self.bus2

        return  f'New \"Line.{self.prefix_name}_{self.line}" phases={self.phases} ' \
        f'bus1="{self.bus1}.{self.bus_nodes}" bus2="{self.bus2}.{self.bus_nodes}" ' \
        f'{linecode} length={self.length:.5f} ' \
        f'units={self.units}'

    def pattern_switch(self):

        if self.prefix_name == "CMT":
            if ordem_pacs() == 'Invertida': #define a ordem dos buses de acordo com o bus inicial
                self.bus2, self.bus1 = self.bus1, self.bus2 
            else:
                self.bus1, self.bus2 = self.bus1, self.bus2

        if self.estado == 'A':
            return  f'!New \"Line.{self.prefix_name}_{self.line}" phases={self.phases} ' \
            f'bus1="{self.bus1}.{self.bus_nodes}" bus2="{self.bus2}.{self.bus_nodes}" ' \
            f'r1={self.r1} r0={self.r0} x1={self.x1} x0={self.x0} c1={self.c1} c0={self.c0}  ' \
            f'switch = {self.switch} length={self.length:.5f}' \
            f'\n !Chave MT em estado ABERTO!'
            
        else:
            return  f'New \"Line.{self.prefix_name}_{self.line}" phases={self.phases} ' \
            f'bus1="{self.bus1}.{self.bus_nodes}" bus2="{self.bus2}.{self.bus_nodes}" ' \
            f'r1={self.r1} r0={self.r0} x1={self.x1} x0={self.x0} c1={self.c1} c0={self.c0}  ' \
            f'switch = {self.switch} length={self.length:.5f}'

    def full_string(self) -> str:
        
        if f'{self.prefix_name}_{self.line}' in elem_isolados(): #remove as linhas isoladas
        #if "BT" in self.prefix_name and (self.transformer in list_dsativ or self.transformer not in dicionario_kv.keys()):
            return("")

        if self.prefix_name == "CMT" or self.prefix_name == "CBT":
            return self.pattern_switch()
        else:
            return self.pattern_segment()


    def __repr__(self):

        if f'{self.prefix_name}_{self.line}' in elem_isolados(): #remove as linhas isoladas
        #if "BT" in self.prefix_name and (self.transformer in list_dsativ or self.transformer not in dicionario_kv.keys()):
            return("")

        if self.prefix_name == "CMT" or self.prefix_name == "CBT":
            return self.pattern_switch()
        else:
            return self.pattern_segment()


    @staticmethod
    def _process_static(line_, value):
        """Static method to process the static configuration for a Line object.

        Args:
            line_ (object): A Line object being updated.
            value (dict): A dictionary containing the static configuration.

        This method processes the static configuration by iterating through the
        key-value pairs of the 'value' dictionary and directly setting the
        corresponding attribute on the Line object with the static value.
        """
        for static_key, static_value in value.items():
            setattr(line_, f"_{static_key}", static_value)


    @staticmethod
    def _process_direct_mapping(line_, value, row):
        """Static method to process the direct mapping configuration for a Line object.

        Args:
            line_ (object): A Line object being updated.
            value (dict): A dictionary containing the direct mapping configuration.
            row (pd.Series): A row from the GeoDataFrame containing line-related data.

        This method processes the direct mapping configuration by iterating through the
        key-value pairs of the 'value' dictionary and directly setting the corresponding
        attribute on the Line object using the value from the row.
        """
        for mapping_key, mapping_value in value.items():
            setattr(line_, f"_{mapping_key}", row[mapping_value])

    @staticmethod
    def _process_indirect_mapping(line_, value, row):
        """Static method to process the indirect mapping configuration for a line object.

        Args:
            line_ (object): A line object being updated.
            value (dict): A dictionary containing the indirect mapping configuration.
            row (pd.Series): A row from the GeoDataFrame containing line-related data.

        This method processes the indirect mapping configuration by iterating through the
        key-value pairs of the 'value' dictionary. If the value is a list, it treats the
        first element as a parameter name and the second element as a function name. The
        method then retrieves the parameter value from the row and calls the specified
        function with that parameter value. The result is then set as an attribute on the
        line object.

        If the value is not a list, the method directly sets the corresponding attribute on
        the line object using the value from the row.
        """
        for mapping_key, mapping_value in value.items():
            if isinstance(mapping_value, list):
                param_name, function_name = mapping_value
                function_ = globals()[function_name]
                param_value = row[param_name]
                setattr(line_, f"_{mapping_key}", function_(str(param_value)))
            else:
                setattr(line_, f"_{mapping_key}", row[mapping_value])

    @staticmethod
    def _process_calculated(line_, value, row):
        """Static method to process the calculated mapping configuration for a Line object.

        Args:
            line_ (object): A Line object being updated.
            value (dict): A dictionary containing the direct mapping configuration.
            row (pd.Series): A row from the GeoDataFrame containing line-related data.

        This method processes the direct mapping configuration by iterating through the
        key-value pairs of the 'value' dictionary and directly setting the corresponding
        attribute on the Line object using the value from the row.
        """
        for mapping_key, mapping_value in value.items():

            expression = ""
            for item in mapping_value:
                if isinstance(item, str) and any(char.isalpha() for char in item):

                    expression = f'{expression} {row[item]}'
                else:
                    expression = f'{expression}{item}'

            param_value = eval(expression)
            if mapping_key == 'length' and getattr(line_,"_prefix_name") == 'RBT':
                length = Line.limitar_ramal(param_value) #settings(limitar tamanho do ramal em 30m)
                setattr(line_, f"_{mapping_key}", length)
            else:
                setattr(line_, f"_{mapping_key}", param_value)



    @staticmethod
    def _create_line_from_row(line_config, row):

        line_ = Line()

        for key, value in line_config.items():
            if key == "calculated":
                line_._process_calculated(line_, value, row)
            elif key == "direct_mapping":
                line_._process_direct_mapping(line_, value, row)
            elif key == "indirect_mapping":
                line_._process_indirect_mapping(line_, value, row)
            elif key == "static":
                line_._process_static(line_, value)
        return line_

    @staticmethod
    def create_line_from_json(json_data: Any, dataframe: gpd.geodataframe.GeoDataFrame, entity: str, pastadesaida:str=""):

        lines = []
        line_config = json_data['elements']['Line'][entity]
        progress_bar = tqdm(dataframe.iterrows(), total=len(dataframe), desc="Line", unit=" lines", ncols=100)
        for _, row in progress_bar:
            line_ = Line._create_line_from_row(line_config, row)

            lines.append(line_)
            progress_bar.set_description(f"Processing Line {entity} {_ + 1}")

        file_name = create_output_file(lines, line_config["arquivo"], feeder=line_.feeder, output_folder=pastadesaida)

        return lines, file_name
