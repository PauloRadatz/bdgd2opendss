# -*- encoding: utf-8 -*-
"""
 * Project Name: main.py
 * Created by migueldcga
 * Date: 02/11/2023
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
import numpy

import geopandas as gpd
from tqdm import tqdm

from bdgd2opendss.model.Converter import convert_ttranf_phases, convert_tfascon_bus, convert_tten, convert_ttranf_windings, convert_tfascon_conn, convert_tpotaprt, convert_tfascon_phases,  convert_tfascon_bus_prim,  convert_tfascon_bus_sec,  convert_tfascon_bus_terc, convert_tfascon_phases_trafo
from bdgd2opendss.model.Circuit import kv
from bdgd2opendss.core.Utils import create_output_file

from dataclasses import dataclass

dicionario_kv = {}
list_dsativ = []
# mtkv = []

@dataclass
class Transformer:

    _feeder: str = ""
    _fase: str =""
    _bus1: str = ""
    _bus2: str = ""
    _bus3: str = ""
    _suffix_bus1: str = ""
    _suffix_bus2: str = ""
    _suffix_bus3: str = ""
    _transformer: str = ""
    _kv1: float = 0.0
    _kv2: float = 0.0
    _kv3: float = 0.0


    _tap: float = 0.0
    _MRT: int = 0
    _Tip_Lig: str = ""
    _sit_ativ: str = ""


    _phases: int = 0
    _bus1_nodes: str = ""
    _bus2_nodes: str = ""
    _bus3_nodes: str = ""
    _windings: str =  ""
    _conn_p: str = ""
    _conn_s: str = ""
    _conn_t: str = ""
    _kvas: float = 0.0
    _noloadloss: float = 0.0
    _totalloss: float = 0.0


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
    def transformer(self):
        return self._transformer

    @transformer.setter
    def transformer(self, value):
        self._transformer = value

    @property
    def kvas(self):
        return self._kvas

    @kvas.setter
    def kvas(self, value):
        self._kvas = value

    @property
    def tap(self):
        return self._tap

    @tap.setter
    def tap(self, value):
        self._tap = value

    @property
    def MRT(self):
        return self._MRT

    @MRT.setter
    def MRT(self, value):
        self._MRT = value

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
    def kv2(self):
        return self._kv2

    @kv2.setter
    def kv2(self, value):
        self._kv2 = value

    @property
    def kv3(self):
        return self._kv3

    @kv3.setter
    def kv3(self, value):
        self._kv3 = value


    @property
    def windings(self):
        return self._windings

    @windings.setter
    def windings(self, value):
        self._windings = value

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
    def totalloss(self):
        return self._totalloss

    @totalloss.setter
    def totalloss(self, value):
        self._totalloss = value

    @property
    def noloadloss(self):
        return self._noloadloss

    @noloadloss.setter
    def noloadloss(self, value):
        self._noloadloss = value

    @property
    def Tip_Lig(self):
        return self._Tip_Lig

    @Tip_Lig.setter
    def Tip_Lig(self, value):
        self._Tip_Lig = value

    @property
    def fase(self):
        return self._fase

    @fase.setter
    def fase(self, value):
        self._fase = value
    
    @property
    def sit_ativ(self):
        return self._sit_ativ

    @sit_ativ.setter
    def sit_ativ(self, value):
        self._sit_ativ = value
    

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
=
        """
        if self.sit_ativ == "DS":
            return("")

        self.kv1 = float(kv[0])
        if self.MRT == 1:
            if '4' in self.bus3_nodes or self.bus2_nodes == '1.2.4':
                kvs = f'{self.kv1/numpy.sqrt(3):.3f} {self.kv2/2} {self.kv2/2}'
                kvas = f'{self.kvas} {self.kvas} {self.kvas}'
                buses = f'"{self.bus1}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}" "{self.bus3}.{self.bus3_nodes}" '
                conns = f'{self.conn_p} {self.conn_s} {self.conn_t}'
            else:
                kvs = f'{self.kv1/numpy.sqrt(3):.3f} {self.kv2/numpy.sqrt(3):.3f}'
                kvas = f'{self.kvas} {self.kvas} {self.kvas}'
                buses = f'"{self.bus1}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}" "{self.bus3}.{self.bus3_nodes}" '
                conns = f'{self.conn_p} {self.conn_s} {self.conn_t}'
            MRT = self.pattern_MRT()
        else:
            if self.Tip_Lig == 'T':
                kvs = f'{self.kv1} {self.kv2}'
                buses = f'"{self.bus1}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}"'
                kvas = f'{self.kvas} {self.kvas}'
                conns = f'{self.conn_p} {self.conn_s}'
            elif '4' in self.bus3_nodes or self.bus2_nodes == '1.2.4':
                if self.phases == 1 or '4' in self.bus1_nodes:
                    kvs = f'{self.kv1/numpy.sqrt(3):.3f} {self.kv2/2} {self.kv2/2}'
                else:
                    kvs = f'{self.kv1} {self.kv2/2} {self.kv2/2}'
                kvas = f'{self.kvas} {self.kvas} {self.kvas}'
                buses = f'"{self.bus1}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}" "{self.bus3}.{self.bus3_nodes}" '
                conns = f'{self.conn_p} {self.conn_s} {self.conn_t}'
            elif len(self.bus3_nodes) == 0 and (len(self.bus2_nodes) == 3 or self.bus2_nodes == '1.2.3'):
                if len(self.bus2_nodes) == 5 and '4' in self.bus2_nodes:
                    if self.phases == 1 or '4' in self.bus1_nodes:
                        kvs = f'{self.kv1/numpy.sqrt(3):.3f} {self.kv2/numpy.sqrt(3):.3f}'
                    else:
                        kvs = f'{self.kv1} {self.kv2/numpy.sqrt(3):.3f}'                    
                else:
                    if self.phases == 1 or '4' in self.bus1_nodes:
                        kvs = f'{self.kv1/numpy.sqrt(3):.3f} {self.kv2}'
                    else:
                        kvs = f'{self.kv1} {self.kv2}'
                buses = f'"{self.bus1}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}"'
                kvas = f'{self.kvas} {self.kvas}'
                conns = f'{self.conn_p} {self.conn_s}'
            MRT = ""
        kva = self.kvas
        # kvas = ' '.join([f'{self.kvas}' for _ in range(self.windings)])
        taps = ' '.join([f'{self.tap}' for _ in range(self.windings)])


        return kvs, buses, conns, kvas, taps, kva, MRT

    def pattern_reactor(self,tip_lig):
        tip_lig = self.Tip_Lig
        if tip_lig == "DA" or tip_lig == "DF":
            return  f'New "Reactor.TRF_{self.transformer}{self.fase}_R" phases=1 bus1="{self.bus2}.4" R=15 X=0 basefreq=60'
        else:
            return  f'New "Reactor.TRF_{self.transformer}_R" phases=1 bus1="{self.bus2}.4" R=15 X=0 basefreq=60'

    def pattern_MRT(self):

        return (f'New "Linecode.LC_MRT_TRF_{self.transformer}_1" nphases=1 basefreq=60 r1=15000 x1=0 units=km normamps=0\n'
                f'New "Linecode.LC_MRT_TRF_{self.transformer}_2" nphases=2 basefreq=60 r1=15000 x1=0 units=km normamps=0\n'
                f'New "Linecode.LC_MRT_TRF_{self.transformer}_3" nphases=3 basefreq=60 r1=15000 x1=0 units=km normamps=0\n'
                f'New "Linecode.LC_MRT_TRF_{self.transformer}_4" nphases=4 basefreq=60 r1=15000 x1=0 units=km normamps=0\n' #alteração feita por Mozart - 26/06 às 11h
                f'New "Line.Resist_MTR_TRF_{self.transformer}" phases=1 bus1="{self.bus1}.{self.bus1_nodes}" bus2="MRT_{self.bus1}TRF_{self.transformer}.{self.bus1_nodes}" linecode="LC_MRT_TRF_{self.transformer}_1" length=0.001 units=km\n')

    def full_string(self) -> str:
        if self.sit_ativ == 'DS':
            return("")
        # if self.conn_p == '1.2.3' or self.conn_p == '1.2.3.4':
        #     phases = 3
        # else:
        #     phases = 1
        self.kvs, self.buses, self.conns, self.kvas, self.taps, kva, MRT= Transformer.adapting_string_variables(self)
 
        return (f'New \"Transformer.TRF_{self.transformer}{self.fase}" phases={self.phases} '
                f'windings={self.windings} '
                f'buses=[{self.buses}] '
                f'conns=[{self.conns}] '
                f'kvs=[{self.kvs}] '
                f'taps=[{self.taps}] '
                f'kvas=[{self.kvas}] '
                f'%loadloss={(float(self.totalloss)-float(self.noloadloss))/(10*float(kva)):.6f} %noloadloss={float(self.noloadloss)/(10*float(kva)):.6f}\n'
                f'{MRT}'
                f'{self.pattern_reactor(self.Tip_Lig)}')

    def __repr__(self):

        if self.sit_ativ == 'DS':
            return("")
    
        self.kvs, self.buses, self.conns, self.kvas, self.taps, kva, MRT= Transformer.adapting_string_variables(self)


        if self.Tip_Lig == 'DF' or self.Tip_Lig == 'DA':
            return (f'New \"Transformer.TRF_{self.transformer}{self.fase}" phases={self.phases} '
                f'windings={self.windings} '
                f'buses=[{self.buses}] '
                f'conns=[{self.conns}] '
                f'kvs=[{self.kvs}] '
                f'taps=[{self.taps}] '
                f'kvas=[{self.kvas}] '
                f'%loadloss={(self.totalloss-self.noloadloss)/(10*float(kva)):.6f} %noloadloss={self.noloadloss/(10*float(kva)):.6f}\n'
                f'{MRT}'
                f'{self.pattern_reactor(self.Tip_Lig)}')
        else:
            return (f'New \"Transformer.TRF_{self.transformer}" phases={self.phases} '
                f'windings={self.windings} '
                f'buses=[{self.buses}] '
                f'conns=[{self.conns}] '
                f'kvs=[{self.kvs}] '
                f'taps=[{self.taps}] '
                f'kvas=[{self.kvas}] '
                f'%loadloss={(self.totalloss-self.noloadloss)/(10*kva):.6f} %noloadloss={self.noloadloss/(10*kva):.6f}\n'
                f'{MRT}'
                f'{self.pattern_reactor(self.Tip_Lig)}')


    @staticmethod
    def _process_static(transformer_, value):
        """
        Static method to process the static configuration for a transformer object.

        Args:
            transformer_ (object): A transformer object being updated.
            value (dict): A dictionary containing the static configuration.

        This method processes the static configuration by iterating through the
        key-value pairs of the 'value' dictionary and directly setting the
        corresponding attribute on the transformer object with the static value.
        """
        for static_key, static_value in value.items():
            setattr(transformer_, f"_{static_key}", static_value)


    @staticmethod
    def _process_direct_mapping(transformer_, value, row):
        """
        Static method to process the direct mapping configuration for a transformer object.

        Args:
            transformer_ (object): A transformer object being updated.
            value (dict): A dictionary containing the direct mapping configuration.
            row (pd.Series): A row from the GeoDataFrame containing transformer-related data.

        This method processes the direct mapping configuration by iterating through the
        key-value pairs of the 'value' dictionary and directly setting the corresponding
        attribute on the transformer object using the value from the row.
        """
        for mapping_key, mapping_value in value.items():
            setattr(transformer_, f"_{mapping_key}", row[mapping_value])
            if mapping_key == "transformer":#modificação - 08/08
                dicionario_kv[row[mapping_value]] = getattr(transformer_,"kv2")
            if mapping_key == "sit_ativ" and row[mapping_value] == "DS":
                list_dsativ.append(getattr(transformer_, f'_transformer'))

    @staticmethod
    def _process_indirect_mapping(transformer_, value, row):
        """
        Static method to process the indirect mapping configuration for a transformer object.

        Args:
            transformer_ (object): A transformer object being updated.
            value (dict): A dictionary containing the indirect mapping configuration.
            row (pd.Series): A row from the GeoDataFrame containing transformer-related data.

        This method processes the indirect mapping configuration by iterating through the
        key-value pairs of the 'value' dictionary. If the value is a list, it treats the
        first element as a parameter name and the second element as a function name. The
        method then retrieves the parameter value from the row and calls the specified
        function with that parameter value. The result is then set as an attribute on the
        transformer object.

        If the value is not a list, the method directly sets the corresponding attribute on
        the transformer object using the value from the row.
        """
        for mapping_key, mapping_value in value.items():
            if isinstance(mapping_value, list):
                param_name, function_name = mapping_value
                function_ = globals()[function_name]
                param_value = row[param_name]
                setattr(transformer_, f"_{mapping_key}", function_(str(param_value)))
            else:
                setattr(transformer_, f"_{mapping_key}", row[mapping_value])

    @staticmethod
    def _process_calculated(transformer_, value, row):
        """
        Static method to process the calculated mapping configuration for a transformer object.

        Args:
            transformer_ (object): A transformer object being updated.
            value (dict): A dictionary containing the direct mapping configuration.
            row (pd.Series): A row from the GeoDataFrame containing transformer-related data.

        This method processes the direct mapping configuration by iterating through the
        key-value pairs of the 'value' dictionary and directly setting the corresponding
        attribute on the transformer object using the value from the row.
        """
        for mapping_key, mapping_value in value.items():

            expression = ""
            for item in mapping_value:
                if isinstance(item, str) and any(char.isalpha() for char in item):

                    expression = f'{expression} {row[item]}'
                else:
                    expression = f'{expression}{item}'
            param_value = eval(expression)

            setattr(transformer_, f"_{mapping_key}", param_value)



    @staticmethod
    def _create_transformer_from_row(transformer_config, row):
        transformer_ = Transformer()

        for key, value in transformer_config.items():
            if key == "calculated":
                transformer_._process_calculated(transformer_, value, row)
            elif key == "direct_mapping":
                transformer_._process_direct_mapping(transformer_, value,row)
            elif key == "indirect_mapping":
                transformer_._process_indirect_mapping(transformer_, value,row)
            elif key == "static":
                transformer_._process_static(transformer_, value)
        return transformer_

    @staticmethod
    def create_transformer_from_json(json_data: Any, dataframe: gpd.geodataframe.GeoDataFrame):
        transformers = []
        transformer_config = json_data['elements']['Transformer']['UNTRMT']


        progress_bar = tqdm(dataframe.iterrows(), total=len(dataframe), desc="Transformer", unit=" transformers", ncols=100)
        for _, row in progress_bar:
            transformer_ = Transformer._create_transformer_from_row(transformer_config, row)
            transformers.append(transformer_)
            progress_bar.set_description(f"Processing transformer {_ + 1}")

        file_name = create_output_file(transformers, transformer_config["arquivo"], feeder=transformer_.feeder)

        return transformers, file_name
    
