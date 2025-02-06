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
from typing import Any, Optional
import numpy
from idlelib.pyparse import trans
import geopandas as gpd
from tqdm import tqdm

from bdgd2opendss.model.Converter import convert_ttranf_phases, convert_tfascon_bus, convert_tten, convert_ttranf_windings, convert_tfascon_conn, convert_tpotaprt, convert_tfascon_phases,  convert_tfascon_bus_prim,  convert_tfascon_bus_sec,  convert_tfascon_bus_terc, convert_tfascon_phases_trafo
from bdgd2opendss.model.Circuit import Circuit
from bdgd2opendss.core.Utils import create_output_file, create_df_trafos_vazios, perdas_trafos_abnt, elem_isolados
from bdgd2opendss.core.Settings import settings

from dataclasses import dataclass

dicionario_kv = {}
dicionario_kv_pri = {}
dict_phase_kv = {}
dict_pot_tr = {}
list_dsativ = []

@dataclass
class Transformer:

    _feeder: str = ""
    _fase: str = ""
    _bus1: str = ""
    _bus2: str = ""
    _bus3: str = ""
    _suffix_bus1: str = ""
    _suffix_bus2: str = ""
    _suffix_bus3: str = ""
    _transformer: str = ""
    _kv1: str = ""
    _kv2: float = 0.0
    _kv3: float = 0.0


    _tap: float = 0.0
    _MRT: int = 0
    _Tip_Lig: str = ""
    _sit_ativ: str = ""
    _posse: str = ""
    _coment: str = ""


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
    def sit_ativ(self):
        return self._sit_ativ

    @sit_ativ.setter
    def sit_ativ(self, value):
        self._sit_ativ = value

    @property
    def posse(self):
        return self._posse

    @posse.setter
    def posse(self, value: str):
        self._posse = value
    
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

        if self.conn_p == 'Wye' and (int(self.phases) == 1 or '4' in self.bus1_nodes):
            if self.kv2 > 1: #TODO se já não for tensão de linha*** verificar esse se
                self.kv1 = f'{float(self.kv1)/numpy.sqrt(3):.13f}'
                if self.conn_s == 'Wye':
                    self.kv2 = f'{float(self.kv2)/numpy.sqrt(3):.13f}'
            else:
                self.kv1 = f'{float(Circuit.kvbase()/numpy.sqrt(3)):.13f}'
        else:
            if self.kv2 < 1:
                self.kv1 = f'{float(Circuit.kvbase())}'
            else:
                if self.conn_s == 'Wye':
                    self.kv2 = f'{float(self.kv2)/numpy.sqrt(3):.13f}'
                    
        if self.MRT == 1: #Condições usadas pela geoperdas de declarar as tensões de secundário e primário de TRAFO
            if '4' in self.bus3_nodes or self.bus2_nodes == '1.2.4':
                kvs = f'{self.kv1} {self.kv2/2} {self.kv2/2}'
                kvas = f'{self.kvas} {self.kvas} {self.kvas}'
                buses = f'"MRT_{self.bus1}TRF_{self.transformer}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}" "{self.bus2}.{self.bus3_nodes}" '
                conns = f'{self.conn_p} {self.conn_s} {self.conn_t}'
            elif len(self.bus3_nodes) == 0 and (len(self.bus2_nodes) == 3 or self.bus2_nodes == '1.2.3'):
                if len(self.bus2_nodes) == 5 and '4' in self.bus2_nodes:
                    kvs = f'{self.kv1} {self.kv2/numpy.sqrt(3):.13f}'                    
                else:
                    kvs = f'{self.kv1} {self.kv2}'
                buses = f'"MRT_{self.bus1}TRF_{self.transformer}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}"'
                kvas = f'{self.kvas} {self.kvas}'
                conns = f'{self.conn_p} {self.conn_s}'
            else:
                kvs = f'{self.kv1} {self.kv2}'
                buses = f'"MRT_{self.bus1}TRF_{self.transformer}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}"'
                kvas = f'{self.kvas} {self.kvas}'
                conns = f'{self.conn_p} {self.conn_s}'
            MRT = self.pattern_MRT()
        else: 
            if self.Tip_Lig == 'T':
                kvs = f'{self.kv1} {self.kv2}'
                buses = f'"{self.bus1}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}"'
                kvas = f'{self.kvas} {self.kvas}'
                conns = f'{self.conn_p} {self.conn_s}'
            elif '4' in self.bus3_nodes or self.bus2_nodes == '1.2.4':
                kvs = f'{self.kv1} {self.kv2/2} {self.kv2/2}'
                kvas = f'{self.kvas} {self.kvas} {self.kvas}'
                buses = f'"{self.bus1}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}" "{self.bus2}.{self.bus3_nodes}" '
                conns = f'{self.conn_p} {self.conn_s} {self.conn_t}'
            elif len(self.bus3_nodes) == 0 and (len(self.bus2_nodes) == 3 or self.bus2_nodes == '1.2.3'):
                if len(self.bus2_nodes) == 5 and '4' in self.bus2_nodes:
                    kvs = f'{self.kv1} {self.kv2/numpy.sqrt(3):.13f}'                    
                else:
                    kvs = f'{self.kv1} {self.kv2}'
                buses = f'"{self.bus1}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}"'
                kvas = f'{self.kvas} {self.kvas}'
                conns = f'{self.conn_p} {self.conn_s}'
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

    def pattern_reactor(self):
        return f'New "Reactor.TRF_{self.transformer}_R" phases=1 bus1="{self.bus2}.4" R=15 X=0 basefreq=60'

    def pattern_MRT(self):

        return (f'{self._coment}New "Linecode.LC_MRT_TRF_{self.transformer}_1" nphases=1 basefreq=60 r1=15000 x1=0 units=km normamps=0\n'
                f'{self._coment}New "Linecode.LC_MRT_TRF_{self.transformer}_2" nphases=2 basefreq=60 r1=15000 x1=0 units=km normamps=0\n'
                f'{self._coment}New "Linecode.LC_MRT_TRF_{self.transformer}_3" nphases=3 basefreq=60 r1=15000 x1=0 units=km normamps=0\n'
                f'{self._coment}New "Linecode.LC_MRT_TRF_{self.transformer}_4" nphases=4 basefreq=60 r1=15000 x1=0 units=km normamps=0\n' #alteração feita por Mozart - 26/06 às 11h
                f'{self._coment}New "Line.Resist_MTR_TRF_{self.transformer}" phases=1 bus1="{self.bus1}.{self.bus1_nodes}" bus2="MRT_{self.bus1}TRF_{self.transformer}.{self.bus1_nodes}" linecode="LC_MRT_TRF_{self.transformer}_1" length=0.001 units=km \n')

    def full_string(self) -> str:
        #if self.transformer in elem_isolados():
        # if self.sit_ativ == 'DS':
        #     return("")
        # else:
        if settings.intAdequarTrafoVazio and self.transformer[:-1] in create_df_trafos_vazios(): #settings (comenta os transformadores vazios)
            self._coment = '!'
        else:
            self._coment = ''

        self.kvs, self.buses, self.conns, self.kvas, self.taps, kva, MRT = Transformer.adapting_string_variables(self)

        if settings.intAdequarTapTrafo: #settings (adequar taps de transformadores)
            if len(self.taps) < 8:
                taps = f'taps=[1.0 {self.taps[0:3]}] '
            else:
                taps = f'taps=[1.0 {self.taps[0:3]} {self.taps[8:11]}] '
        else:
            taps = ""

        if settings.intNeutralizarTrafoTerceiros and self.posse != 'PD': #settings (neutraliza transformadores de terceiros)
            self.totalloss = 0
            self.noloadloss = 0
        if settings.intUsaTrafoABNT: #settings (configuração para utilização de perdas da ABNT 5440)
            if self.conn_p == 'Wye' and (int(self.phases) == 1 or '4' in self.bus1_nodes):
                kv1 = Circuit.kvbase()
            else:
                kv1 = float(self.kv1)
            if self.conn_p == 'Delta' and self.phases == '1':
                self.totalloss = float(perdas_trafos_abnt(2,kv1,kva,'totalloss'))
                self.noloadloss = float(perdas_trafos_abnt(2,kv1,kva,'noloadloss'))
            else:
                self.totalloss = float(perdas_trafos_abnt(self.phases,kv1,kva,'totalloss'))
                self.noloadloss = float(perdas_trafos_abnt(self.phases,kv1,kva,'noloadloss'))

        return (f'{self._coment}New \"Transformer.TRF_{self.transformer}" phases={self.phases} '
            f'windings={self.windings} '
            f'buses=[{self.buses}] '
            f'conns=[{self.conns}] '
            f'kvs=[{self.kvs}] '
            f'{taps}'
            f'kvas=[{self.kvas}] '
            f'%loadloss={(float(self.totalloss)-float(self.noloadloss))/(10*float(kva)):.6f} %noloadloss={self.noloadloss/(10*float(kva)):.6f}\n'
            f'{self._coment}{self.pattern_reactor()}\n'
            f'{MRT}')
                
    def __repr__(self):
        if self.sit_ativ == 'DS':
            return("")
        else:
            self.kvs, self.buses, self.conns, self.kvas, self.taps, kva, MRT= Transformer.adapting_string_variables(self)
 
            return (f'New \"Transformer.TRF_{self.transformer}" phases={self.phases} '
                f'windings={self.windings} '
                f'buses=[{self.buses}] '
                f'conns=[{self.conns}] '
                f'kvs=[{self.kvs}] '
                f'taps=[{self.taps}] '
                f'kvas=[{self.kvas}] '
                f'%loadloss={(float(self.totalloss)-float(self.noloadloss))/(10*float(kva)):.6f} %noloadloss={float(self.noloadloss)/(10*float(kva)):.6f}\n'
                f'{MRT}'
                f'{self.pattern_reactor()}')
        
    def sec_phase_kv(transformer:Optional[str] = None,kv2:Optional[float] = None,bus2_nodes:Optional[str] = None,bus3_nodes:Optional[str] = None, trload:Optional[str] = None, tip_trafo:Optional[str] = None): #retorna um dicionario de tensões de fase para cargas de acordo com critérios do Geoperdas
        if trload == None:
            if bus3_nodes != 'XX' and (kv2 == 0.24 or kv2 == 0.44):
                dict_phase_kv[transformer] = kv2/2 
            elif kv2 != 0.38 and not ((len(bus2_nodes) == 5 and '4' in bus2_nodes) or (len(bus2_nodes) == 3 and '4' not in bus2_nodes)):
                dict_phase_kv[transformer] = kv2  
            else: 
                dict_phase_kv[transformer] = kv2/numpy.sqrt(3)
        else:
            return(dict_phase_kv[trload])
        
    def sec_line_kv(transformer:Optional[str] = None,kv2:Optional[float] = None, trload:Optional[str] = None): #retornar um dicionario de tensões de linha para a carga e acordo com critérios do Geoperdas
        if trload == None:
            dicionario_kv[transformer] = kv2
        else:
            return(dicionario_kv[trload])
    
    def dict_pot_tr(transformer:Optional[str] = None,kva:Optional[float] = None, trload:Optional[str] = None): #retornar um dicionario de tensões de linha para a carga e acordo com critérios do Geoperdas
        if trload == None:
            dict_pot_tr[transformer] = kva
        else:
            return(dict_pot_tr[trload])
    
    @staticmethod    
    def dict_kv():
        return(dicionario_kv)
    
    @staticmethod    
    def list_dsativ():
        return(list_dsativ)
    
    def dict_kv_pri():
        return(dicionario_kv_pri)
        
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
                Transformer.sec_line_kv(transformer=row[mapping_value][:-1],kv2=getattr(transformer_,"kv2"))
            if mapping_key == "sit_ativ" and row[mapping_value] == "DS":
                list_dsativ.append(getattr(transformer_, f'_transformer')[:-1])

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
                if mapping_key == 'bus3_nodes':
                    Transformer.sec_phase_kv(getattr(transformer_, f'_transformer')[:-1],getattr(transformer_, f'_kv2'),getattr(transformer_, f'_bus2_nodes'),function_(str(param_value)))
                if mapping_key == 'kvas': #settings - limitar cargas BT (potencia atv do trafo): cria dicionário de trafos/potências
                    Transformer.dict_pot_tr(getattr(transformer_, f'_transformer')[:-1],function_(str(param_value)))
                if mapping_key == 'kv1':
                    dicionario_kv_pri[getattr(transformer_, f'_transformer')] = function_(str(param_value))
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
    #def create_transformer_from_json(json_data: Any, dataframe: gpd.geodataframe.GeoDataFrame, kVbaseObj: Any, pastadesaida: str = ""):
    def create_transformer_from_json(json_data: Any, dataframe: gpd.geodataframe.GeoDataFrame, pastadesaida: str = ""):
        transformers = []
        transformer_config = json_data['elements']['Transformer']['UNTRMT']
        
        # global _kVbase_GLOBAL 
        # _kVbase_GLOBAL = kVbaseObj.MV_kVbase

        progress_bar = tqdm(dataframe.iterrows(), total=len(dataframe), desc="Transformer", unit=" transformers", ncols=100)
        for _, row in progress_bar:
            transformer_ = Transformer._create_transformer_from_row(transformer_config, row)
            transformers.append(transformer_)
            progress_bar.set_description(f"Processing transformer {_ + 1}")

        file_name = create_output_file(transformers, transformer_config["arquivo"], feeder=transformer_.feeder, output_folder=pastadesaida)
        #kVbaseObj.LV_kVbase = dicionario_kv

        return transformers, file_name
        #return kVbaseObj, file_name
    
