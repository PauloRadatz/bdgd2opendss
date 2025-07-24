# -*- encoding: utf-8 -*-
"""
 * Project Name: main.py
 * Created by migueldcga
 * Date: 06/11/2023
 * Time: 23:53
 *
 * Edited by: Mozartdon 
 * Date:25/09/2024
 * Time:
"""
# Não remover a linha de importação abaixo
import copy
import re
from typing import Any

import numpy as np
import geopandas as gpd
from tqdm import tqdm

from bdgd2opendss.model.Converter import convert_ttranf_phases, convert_tfascon_bus, convert_tfascon_phases, convert_tten, convert_ttranf_windings, convert_tfascon_conn, convert_tpotaprt, convert_ptratio
from bdgd2opendss.core.Utils import create_output_file, ordem_pacs, elem_isolados, seq_eletrica
from bdgd2opendss.model.Circuit import Circuit


from dataclasses import dataclass


@dataclass
class RegControl:

    _feeder: str = ""

    _vreg: float =  0.0
    _band: int = 0
    _ptratio: float = 0.0
    _xhl: str =  ""

    _bus1: str = ""
    _bus2: str = ""
    _bus3: str = ""
    _suffix_bus1: str = ""
    _suffix_bus2: str = ""
    _suffix_bus3: str = ""
    _prefix_transformer: str = ""
    _transformer: str = ""
    _ten_lin_se: str = ""
    _kv1: int = 0
    _buses: str = ""
    _phases: int = 0
    _bus1_nodes: str = ""
    _bus2_nodes: str = ""
    _bus3_nodes: str = ""
    _windings: str =  ""
    _conn_p: str = ""
    _conn_s: str = ""
    _conn_t: str = ""
    _kvas: int = 0
    _totalloss: float = 0.0
    _noloadloss: float = 0.0
    _banco: str = ""

    @property
    def feeder(self):
        return self._feeder

    @feeder.setter
    def feeder(self, value):
        self._feeder = value

    @property
    def bus1(self):
        return self._bus1

    @bus1.setter
    def bus1(self, value):
        self._bus1 = value

    @property
    def bus2(self):
        return self._bus2

    @bus2.setter
    def bus2(self, value):
        self._bus2 = value

    @property
    def bus3(self):
        return self._bus3

    @bus3.setter
    def bus3(self, value):
        self._bus3 = value

    @property
    def kvas(self):
        return self._kvas

    @kvas.setter
    def kvas(self, value):
        self._kvas = value

    @property
    def phases(self):
        return self._phases

    @phases.setter
    def phases(self, value):
        self._phases = value

    @property
    def bus1_nodes(self):
        return self._bus1_nodes

    @bus1_nodes.setter
    def bus1_nodes(self, value):
        self._bus1_nodes = value

    @property
    def bus2_nodes(self):
        return self._bus2_nodes

    @bus2_nodes.setter
    def bus2_nodes(self, value):
        self._bus2_nodes = value

    @property
    def bus3_nodes(self):
        return self._bus3_nodes

    @bus3_nodes.setter
    def bus3_nodes(self, value):
        self._bus3_nodes = value

    @property
    def kv1(self):
        return self._kv1

    @kv1.setter
    def kv1(self, value):
        self._kv1 = value

    @property
    def conn_p(self):
        return self._conn_p

    @conn_p.setter
    def conn_p(self, value):
        self._conn_p = value

    @property
    def conn_s(self):
        return self._conn_s

    @conn_s.setter
    def conn_s(self, value):
        self._conn_s = value

    @property
    def conn_t(self):
        return self._conn_t

    @conn_t.setter
    def conn_t(self, value):
        self._conn_t = value

    @property
    def noloadloss(self):
        return self._noloadloss

    @noloadloss.setter
    def noloadloss(self, value):
        self._noloadloss = value

    @property
    def totalloss(self):
        return self._totalloss

    @totalloss.setter
    def totalloss(self, value):
        self._totalloss = value

    @property
    def prefix_transformer(self):
        return self._prefix_transformer

    @prefix_transformer.setter
    def prefix_transformer(self, value):
        self._prefix_transformer = value

    @property
    def transformer(self):
        return self._transformer

    @transformer.setter
    def transformer(self, value):
        self._transformer = value

    @property
    def windings(self):
        return self._windings

    @windings.setter
    def windings(self, value):
        self._windings = value

    @property
    def vreg(self):
        return self._vreg

    @vreg.setter
    def vreg(self, value):
        self._vreg = value

    @property
    def band(self):
        return self._band

    @band.setter
    def band(self, value):
        self._band = value

    @property
    def ptratio(self):
        return self._ptratio

    @ptratio.setter
    def ptratio(self, value):
        self._ptratio = value

    @property
    def xhl(self):
        return self._xhl

    @xhl.setter
    def xhl(self, value):
        self._xhl = value

    @property
    def buses(self):
        return self._buses

    @buses.setter
    def buses(self, value):
        self._buses = value
    
    @property
    def banco(self):
        return self._banco

    @banco.setter
    def banco(self, value):
        self._banco = value


    def adapting_string_variables(self):

        """
        Format and adapt instance variables to create strings for OpenDSS input.

        This method prepares and formats instance variables to be used as strings in OpenDSS input.
        It constructs strings for voltage levels, bus definitions, kVA ratings, and tap settings based
        on the values stored in the object.

        Returns:
            tuple of strings: A tuple containing the following formatted strings:
                - kvs: A string representing voltage levels in kV for different phases.
                - buses: A string representing bus definitions in OpenDSS format.
                - kvas: A string representing kVA ratings
                - taps: A string representing tap settings



            Calling this method will format the variables and return a tuple of strings for OpenDSS input.

        """

        self.kv1 = seq_eletrica(key=self.bus1) #corrigir em caso de erro
        if self.conn_p == 'Wye':
            kvs = f"{self.kv1/np.sqrt(3):.3f} {self.kv1/np.sqrt(3):.3f}"
            kv = self.kv1*1000/np.sqrt(3)
        else:
            kvs = f'{self.kv1} {self.kv1}'
            kv = self.kv1*1000
        
        if self.bus2_nodes == '1.2' or self.bus2_nodes == '2.3' or self.bus2_nodes == '3.1' or self.bus2_nodes == '1.2.3':
            ptratio = self.kv1*10
        else:
            ptratio = self.kv1*10/np.sqrt(3)

        if ordem_pacs() == 'Invertida': #define a ordem dos buses de acordo com o bus inicial
            buses = f'"{self.bus2}.{self.bus2_nodes}" "{self.bus1}.{self.bus1_nodes}"'
        else:
            buses = f'"{self.bus1}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}"'

        kva = self.kvas
        kvas = ' '.join([f'{self.kvas}' for _ in range(self.windings)])        

        return buses, kvas, kvs, kv, kva, ptratio

    def pattern_reactor_reg(self):
        if self.conn_p == "Delta":
            return("")
        else:
            return (f'New "Reactor.TRF_{self.transformer}_P" phases=1 bus1="{self.bus1}.4" R=15 X=0 basefreq=60 \n'
                f'New "Reactor.TRF_{self.transformer}_S" phases=1 bus1="{self.bus2}.4" R=15 X=0 basefreq=60'
        )

    def full_string(self) -> str:
        if f"REG_{self.transformer}" in elem_isolados():
            return("") 

        if self.buses == "":
            self.buses, self.kvas, self.kvs, kv, kva, ptratio = RegControl.adapting_string_variables(self)
        
        try: #trata erros numéricos
            #loadloss = f'{(self.totalloss - self.noloadloss)/(1000*kva)}'
            #noloadloss = f'{self.noloadloss/(1000*kva)}'
            loadloss = f'{(self.totalloss - self.noloadloss)/(10*kva)}' #valor correto pois é em porcentagem 100/1000 - Proggeoperdas usa no calculo de perdas
            noloadloss = f'{self.noloadloss/(10*kva)}'
        except ZeroDivisionError as e:
            print(f"An error occurred: {str(e)}")
            loadloss = float('nan')
            noloadloss = float('nan')
            pass
        except ValueError as e:
            print(f"An error occurred: {str(e)}")
            loadloss = float('nan')
            noloadloss = float('nan')
            pass
        
        return(
    f'New \"Transformer.REG_{self.transformer}" phases={self.phases} '
    f'windings={self.windings} '
    f'buses=[{self.buses}] '
    f'conns=[{self.conn_p} {self.conn_s} {self.conn_t}] '
    f'kvs=[{self.kvs}] '
    f'kvas=[{self.kvas}] '
    f'xhl={self.xhl} '
    f'%loadloss={loadloss} %noloadloss={noloadloss}'
    f'\nNew \"Regcontrol.REG_{self.transformer}" transformer="REG_{self.transformer}" '
    f'winding={self.windings} '
    f'vreg={self.vreg*100} '
    f'band={self.band} '
    f'ptratio={ptratio} \n'
    f'{self.pattern_reactor_reg()}')
    
    def __repr__(self):
        if f"REG_{self.transformer}" in elem_isolados():
            return("") 

        if self.buses == "":
            self.buses, self.kvas, self.kvs, kv, kva, ptratio = RegControl.adapting_string_variables(self)
        
        return(
    f'New \"Transformer.REG_{self.transformer}" phases={self.phases} '
    f'windings={self.windings} '
    f'buses=[{self.buses}] '
    f'conns=[{self.conn_p} {self.conn_s} {self.conn_t}] '
    f'kvs=[{self.kvs}] '
    f'kvas=[{self.kvas}] '
    f'xhl={self.xhl} '
    f'%loadloss={(self.totalloss - self.noloadloss)/(1000*kva)} %noloadloss={self.noloadloss/(1000*kva)}'
    f'\nNew \"Regcontrol.REG_{self.transformer}" transformer="REG_{self.transformer}" '
    f'winding={self.windings} '
    f'vreg={self.vreg*100} '
    f'band={self.band} '
    f'ptratio={ptratio} \n'
    f'{self.pattern_reactor_reg()}')


    @staticmethod
    def _process_static(regcontrol_, value):
        """
        Static method to process the static configuration for a regcontrol object.

        Args:
            regcontrol_ (object): A regcontrol object being updated.
            value (dict): A dictionary containing the static configuration.

        This method processes the static configuration by iterating through the
        key-value pairs of the 'value' dictionary and directly setting the
        corresponding attribute on the regcontrol object with the static value.
        """
        for static_key, static_value in value.items():
            setattr(regcontrol_, f"_{static_key}", static_value)


    @staticmethod
    def _process_direct_mapping(regcontrol_, value, row):
        """
        Static method to process the direct mapping configuration for a regcontrol object.

        Args:
            regcontrol_ (object): A regcontrol object being updated.
            value (dict): A dictionary containing the direct mapping configuration.
            row (pd.Series): A row from the GeoDataFrame containing regcontrol-related data.

        This method processes the direct mapping configuration by iterating through the
        key-value pairs of the 'value' dictionary and directly setting the corresponding
        attribute on the regcontrol object using the value from the row.
        """
        for mapping_key, mapping_value in value.items():
            setattr(regcontrol_, f"_{mapping_key}", row[mapping_value])
            if mapping_key == 'banco' and row[mapping_value] == '0':
                setattr(regcontrol_, f"_prefix_transformer", row[mapping_value])

    @staticmethod
    def _process_indirect_mapping(regcontrol_, value, row):
        """
        Static method to process the indirect mapping configuration for a regcontrol object.

        Args:
            regcontrol_ (object): A regcontrol object being updated.
            value (dict): A dictionary containing the indirect mapping configuration.
            row (pd.Series): A row from the GeoDataFrame containing regcontrol-related data.

        This method processes the indirect mapping configuration by iterating through the
        key-value pairs of the 'value' dictionary. If the value is a list, it treats the
        first element as a parameter name and the second element as a function name. The
        method then retrieves the parameter value from the row and calls the specified
        function with that parameter value. The result is then set as an attribute on the
        regcontrol object.

        If the value is not a list, the method directly sets the corresponding attribute on
        the regcontrol object using the value from the row.
        """
        for mapping_key, mapping_value in value.items():
            if isinstance(mapping_value, list):
                param_name, function_name = mapping_value
                function_ = globals()[function_name]
                # if mapping_key == 'kv1':
                #     param_value = seq_eletrica(key=getattr(regcontrol_, f'_bus1')) #pegando a tensão através da sequência elétrica
                # else:
                #     param_value = row[param_name]
                param_value = row[param_name]
                setattr(regcontrol_, f"_{mapping_key}", function_(str(param_value)))
            else:
                setattr(regcontrol_, f"_{mapping_key}", row[mapping_value])

    @staticmethod
    def _process_calculated(regcontrol_, value, row):
        """
        Static method to process the calculated mapping configuration for a regcontrol object.

        Args:
            regcontrol_ (object): A regcontrol object being updated.
            value (dict): A dictionary containing the direct mapping configuration.
            row (pd.Series): A row from the GeoDataFrame containing regcontrol-related data.

        This method processes the direct mapping configuration by iterating through the
        key-value pairs of the 'value' dictionary and directly setting the corresponding
        attribute on the regcontrol object using the value from the row.
        """
        for mapping_key, mapping_value in value.items():

            expression = ""
            for item in mapping_value:
                if isinstance(item, str) and any(char.isalpha() for char in item):
                    expression = f'{expression} {row[item]}'
                else:
                    expression = f'{expression}{item}'
            param_value = eval(expression)

            setattr(regcontrol_, f"_{mapping_key}", param_value)



    @staticmethod
    def _create_regcontrol_from_row(regcontrol_config, row):
        regcontrol_ = RegControl()

        for key, value in regcontrol_config.items():
            if key == "static":
                regcontrol_._process_static(regcontrol_, value)
            elif key == "direct_mapping":
                regcontrol_._process_direct_mapping(regcontrol_, value,row)
            elif key == "indirect_mapping":
                regcontrol_._process_indirect_mapping(regcontrol_, value,row)
            elif key == "calculated":
                regcontrol_._process_calculated(regcontrol_, value, row)

        return regcontrol_

    @staticmethod
    def create_regcontrol_from_json(json_data: Any, dataframe: gpd.geodataframe.GeoDataFrame, pastadesaida: str=""):
        regcontrols = []
        regcontrol_config = json_data['elements']['RegControl']['EQRE']

        progress_bar = tqdm(dataframe.iterrows(), total=len(dataframe), desc="RegControl", unit=" regcontrols", ncols=100)
        for _, row in progress_bar:
            regcontrol_ = RegControl._create_regcontrol_from_row(regcontrol_config, row)
            regcontrols.append(regcontrol_)
            progress_bar.set_description(f"Processing regcontrol {_ + 1}")

        file_name = create_output_file(regcontrols, regcontrol_config["arquivo"], feeder=regcontrol_.feeder, output_folder=pastadesaida)

        return regcontrols, file_name
