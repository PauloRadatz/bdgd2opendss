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
import math
import numpy
import geopandas as gpd
from tqdm import tqdm
import csv
import pandas as pd
import os

from bdgd2opendss.model.Converter import convert_ttranf_phases, convert_tfascon_bus, convert_tten, convert_ttranf_windings, convert_tfascon_conn, convert_tpotaprt, convert_tfascon_phases,  convert_tfascon_bus_prim,  convert_tfascon_bus_sec,  convert_tfascon_bus_terc, convert_tfascon_phases_trafo
from bdgd2opendss.model.Circuit import Circuit
from bdgd2opendss.core.Utils import create_output_file, create_output_folder, create_df_trafos_vazios, perdas_trafos_abnt, elem_isolados
from bdgd2opendss.core.Settings import settings

from dataclasses import dataclass

dicionario_kv = {}
dicionario_kv_pri = {}
dict_phase_kv = {}
dict_pot_tr = {}
dict_available_phases = {}
list_dsativ = []
list_posse = []
list_reactors = []
@dataclass
class Transformer:

    @staticmethod
    def reset_state():
        """Resets global state for a new feeder/circuit."""
        global dicionario_kv, dicionario_kv_pri, dict_phase_kv, dict_pot_tr, dict_available_phases, list_dsativ, list_posse, list_reactors
        dicionario_kv.clear()
        dicionario_kv_pri.clear()
        dict_phase_kv.clear()
        dict_pot_tr.clear()
        dict_available_phases.clear()
        list_dsativ.clear()
        list_posse.clear()
        list_reactors.clear()

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
        """Standardizes voltages and identifiers for OpenDSS generation."""
        # Ensure phases is an integer for comparisons
        try:
            self.phases = int(self.phases)
        except (ValueError, TypeError):
            self.phases = 1

        # 2. Enforce realistic phase constraints for Open-Delta and asymmetrical multi-tank banks
        # Single-phase components (M, MT) should retain phases=1 even if connected phase-to-phase.
        # This prevents OpenDSS from dividing the kV rating by sqrt(3) or 2 when interpreting connections.
        actual_phases = [n for n in self.bus1_nodes.split('.') if n in ['1', '2', '3']]
        if self.phases > 1 and len(actual_phases) > 0:
            self.phases = len(actual_phases)

        # NOTE: Do NOT normalize self.transformer here. The BDGD uses trailing
        # letter suffixes (e.g. 14339896A, 14339896B, 14339896C) to distinguish
        # individual units of a multi-tank bank. Stripping those suffixes at
        # output-generation time causes all units to collide into the same
        # OpenDSS element name (duplicate element warning). Normalization for
        # dictionary lookups is done in _process_direct_mapping /
        # _process_indirect_mapping. Reactor deduplication in full_string
        # already strips the trailing letter when needed.

        # 3. Handle Voltage Bases
        try:
            v1_val = float(self.kv1)
            v2_val = float(self.kv2)
        except (ValueError, TypeError):
            v1_val = float(Circuit.kvbase()) if hasattr(Circuit, 'kvbase') else 13.8
            v2_val = 0.22

        # Detection of Phase-to-Neutral primary connection
        # If the transformer is 1-phase and connected to a single phase (1, 2, or 3) and a neutral (4, 0 or None)
        # the rated voltage in DSS should be the phase-to-neutral voltage.
        primary_nodes = self.bus1_nodes.split('.')
        is_phase_neutral = (self.phases == 1 and
                            any(n in ['1', '2', '3'] for n in primary_nodes) and
                            (any(n in ['0', '4'] for n in primary_nodes) or len(primary_nodes) == 1))

        if is_phase_neutral and v1_val > 1.0:
            v1_final = v1_val / math.sqrt(3)
        else:
            v1_final = v1_val

        v2_final = v2_val

        # 4. Handle MRT and Tip_Lig logic for OpenDSS string formatting
        # Standard Split-phase 120/240V detection
        is_split_phase = (str(self.Tip_Lig) in ['MT', '2', '2.0', 2] or 
                          '4' in self.bus3_nodes or 
                          self.bus2_nodes == '1.2.4')

        if self.MRT == 1: 
            if is_split_phase:
                # Center-tap split-phase (120/240V) -> 0.12 0.12
                # Each half-winding = full secondary line voltage / 2
                half_kv = round(v2_val / 2, 3)
                kvs = f'{v1_final:.3f} {half_kv:.3f} {half_kv:.3f}'
                kvas = f'{self.kvas} {self.kvas} {self.kvas}'
                buses = f'"MRT_{self.bus1}TRF_{self.transformer}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}" "{self.bus2}.{self.bus3_nodes}" '
                conns = f'{self.conn_p} {self.conn_s} {self.conn_t}'
            elif len(self.bus3_nodes) == 0 and (len(self.bus2_nodes) == 3 or self.bus2_nodes == '1.2.3'):
                kvs = f'{v1_final:.3f} {v2_val:.3f}'
                buses = f'"MRT_{self.bus1}TRF_{self.transformer}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}"'
                kvas = f'{self.kvas} {self.kvas}'
                conns = f'{self.conn_p} {self.conn_s}'
            else:
                kvs = f'{v1_final:.3f} {v2_final:.3f}'
                buses = f'"MRT_{self.bus1}TRF_{self.transformer}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}"'
                kvas = f'{self.kvas} {self.kvas}'
                conns = f'{self.conn_p} {self.conn_s}'
            MRT = self.pattern_MRT()
        else:
            if self.Tip_Lig == 'T':
                kvs = f'{v1_final:.3f} {v2_final:.3f}'
                buses = f'"{self.bus1}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}"'
                kvas = f'{self.kvas} {self.kvas}'
                conns = f'{self.conn_p} {self.conn_s}'
            elif is_split_phase:
                # Standard Split-phase 120/240V -> 0.12 0.12
                # Each half-winding = full secondary line voltage / 2
                half_kv = round(v2_val / 2, 3)
                kvs = f'{v1_final:.3f} {half_kv:.3f} {half_kv:.3f}'
                kvas = f'{self.kvas} {self.kvas} {self.kvas}'
                buses = f'"{self.bus1}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}" "{self.bus2}.{self.bus3_nodes}" '
                conns = f'{self.conn_p} {self.conn_s} {self.conn_t}'
                self.windings = 3
            else:
                kvs = f'{v1_final:.3f} {v2_final:.3f}'
                buses = f'"{self.bus1}.{self.bus1_nodes}" "{self.bus2}.{self.bus2_nodes}"'
                kvas = f'{self.kvas} {self.kvas}'
                conns = f'{self.conn_p} {self.conn_s}'
            MRT = ""

        # Update final attributes for basic output
        self.kv1 = f"{v1_final:.3f}"
        self.kv2 = f"{v2_final:.3f}"
        kva = self.kvas
        taps = ' '.join([f'{self.tap}' for _ in range(self.windings)])

        # Update voltage base dictionaries for output generation
        id_tr = Transformer.normalize_trafo_id(self.transformer)
        
        # dicionario_kv: Nominal Line-to-Line bases (e.g., 0.22 kV)
        dicionario_kv[id_tr] = v2_val
        
        # dict_phase_kv: Nominal Phase-to-Neutral bases (e.g., 0.11 kV or 0.127 kV)
        if is_split_phase:
            dict_phase_kv[id_tr] = v2_val / 2.0
        elif self.phases == 3:
            dict_phase_kv[id_tr] = v2_val / math.sqrt(3)
        else:
            dict_phase_kv[id_tr] = v2_val

        return kvs, buses, conns, kvas, taps, kva, MRT

    def pattern_reactor(self):
        return f'New "Reactor.TRF_{self.transformer}_R" phases=1 bus1={self.bus2}.4 R=15 X=0 basefreq=60'

    def pattern_MRT(self):
        Transformer.coords_MRT(self)
        return (f'{self._coment}New "Linecode.LC_MRT_TRF_{self.transformer}_1" nphases=1 basefreq=60 r1=15000 x1=0 units=km normamps=0\n'
                f'{self._coment}New "Linecode.LC_MRT_TRF_{self.transformer}_2" nphases=2 basefreq=60 r1=15000 x1=0 units=km normamps=0\n'
                f'{self._coment}New "Linecode.LC_MRT_TRF_{self.transformer}_3" nphases=3 basefreq=60 r1=15000 x1=0 units=km normamps=0\n'
                f'{self._coment}New "Linecode.LC_MRT_TRF_{self.transformer}_4" nphases=4 basefreq=60 r1=15000 x1=0 units=km normamps=0\n' #alteração feita por Mozart - 26/06 às 11h
                f'{self._coment}New "Line.Resist_MRT_TRF_{self.transformer}" phases=1 bus1="{self.bus1}.{self.bus1_nodes}" bus2="MRT_{self.bus1}TRF_{self.transformer}.{self.bus1_nodes}" linecode="LC_MRT_TRF_{self.transformer}_1" length=0.001 units=km \n')

    def coords_MRT(self):
        global output
        output_directory = create_output_folder(self.feeder,output_folder=output)
        path = os.path.join(output_directory,"buscoords.csv")
        df = pd.read_csv(path)
        if self.bus1 in df['PAC'].tolist():
            result = df[df['PAC'] == self.bus1]
            dados = [
                [f"MRT_{self.bus1}TRF_{self.transformer}",result.loc[result.index[0],'long'],result.loc[result.index[0],'lat']]
            ]
            with open(path, mode="a", newline='') as arquivo:
                escritor = csv.writer(arquivo)
                escritor.writerows(dados)
        else:
            ...

    def full_string(self) -> str:
        if self.transformer in elem_isolados():
            return("")

        if settings.intAdequarTrafoVazio and self.transformer[:-1] in create_df_trafos_vazios(): #settings (comenta os transformadores vazios)
            self._coment = '!'
        else:
            self._coment = ''

        self.kvs, self.buses, self.conns, self.kvas, self.taps, kva, MRT = Transformer.adapting_string_variables(self)

        if settings.intAdequarTapTrafo: #settings (adequar taps de transformadores)
            if self.windings > 2:
                taps = f'taps=[1.0 {self.tap} {self.tap}] '
            else:
                taps = f'taps=[1.0 {self.tap}] '
        else:
            taps = ""

        #if settings.intNeutralizarTrafoTerceiros and self.posse != 'PD': #settings (neutraliza transformadores de terceiros)
        if self.posse != 'PD':
            self.totalloss = 0
            self.noloadloss = 0
        if settings.intUsaTrafoABNT: #settings (configuração para utilização de perdas da ABNT 5440)
            if self.conn_p == 'Wye' and (int(self.phases) == 1 or '4' in self.bus1_nodes):
                kv1 = Circuit.kvbase()
            else:
                kv1 = float(self.kv1)
            if self.conn_p == 'Delta' and self.phases == '1' and kva <= 100:
                self.totalloss = float(perdas_trafos_abnt(2,kv1,kva,'totalloss'))
                self.noloadloss = float(perdas_trafos_abnt(2,kv1,kva,'noloadloss'))
            elif kva <= 300:
                self.totalloss = float(perdas_trafos_abnt(self.phases,kv1,kva,'totalloss'))
                self.noloadloss = float(perdas_trafos_abnt(self.phases,kv1,kva,'noloadloss'))
            else:
                pass
        try: #trata erros numéricos
            loadloss = f'{(float(self.totalloss)-float(self.noloadloss))/(10*float(kva)):.6f}'
            noloadloss = f'{self.noloadloss/(10*float(kva)):.6f}'
        except ZeroDivisionError as e:
            print(f"An error occurred: {str(e)}")
            loadloss = float("nan")
            noloadloss = float("nan")
            pass
        except ValueError as e:
            print(f"An error occurred: {str(e)}")
            loadloss = float("nan")
            noloadloss = float("nan")
            pass

        output_trafo = (f'{self._coment}New \"Transformer.TRF_{self.transformer}" phases={self.phases} '
            f'windings={self.windings} '
            f'buses=[{self.buses}] '
            f'conns=[{self.conns}] '
            f'kvs=[{self.kvs}] '
            f'{taps} '
            f'kvas=[{self.kvas}] '
            f'%loadloss={loadloss} %noloadloss={noloadloss}\n')

        trafo_id = self.transformer[:-1] if self.transformer[-1].isalpha() else self.transformer
        if trafo_id not in list_reactors:
            list_reactors.append(trafo_id)
            
        # Add grounding reactor for any Wye secondary (node 4 presence)
        # Skip if it's a split-phase MRT as it has its own logic in MRT pattern
        if '4' in self.bus2_nodes.split('.') and self.MRT == 0:
            return output_trafo + (f'{self._coment}{self.pattern_reactor()}\n'
                                   f'{MRT}')
        else:
            return output_trafo + f'{MRT}'
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
            # 1 - M, 2 - MT, 3 - B, 4 - T, 5 - DA, 6 - DF
            if tip_trafo in ['MT', 2, '2']:
                # Split-phase: declared voltage is double the phase-to-neutral (e.g. 240V -> 120V)
                dict_phase_kv[transformer] = kv2 / 2.0
            elif tip_trafo in ['T', 'DA', 'DF', 4, 5, 6, '4', '5', '6']:
                # Three-phase: phase-to-neutral voltage is line-to-line / sqrt(3)
                dict_phase_kv[transformer] = kv2 / numpy.sqrt(3)
            else:
                # Original logic: Use raw value directly (Monophasic or others)
                dict_phase_kv[transformer] = kv2
        else:
            try:
                kv2 = dict_phase_kv[trload]
            except KeyError:
                kv2 = float('nan')
            return(kv2)

    def sec_line_kv(transformer:Optional[str] = None,kv2:Optional[float] = None, trload:Optional[str] = None, tip_trafo:Optional[str] = None): #retornar um dicionario de tensões de linha para a carga e acordo com critérios do Geoperdas
        if trload == None:
            # Original logic: Use raw value directly
            dicionario_kv[transformer] = kv2
        else:
            try:
                kv2 = dicionario_kv[trload]
            except KeyError:
                kv2 = float('nan')
            return(kv2)

    def dict_pot_tr(transformer:Optional[str] = None,kva:Optional[float] = None, trload:Optional[str] = None): #retornar um dicionario de tensões de linha para a carga e acordo com critérios do Geoperdas
        if trload == None:
            dict_pot_tr[transformer] = kva
        else:
            return(dict_pot_tr[trload])

    @staticmethod
    def dict_kv():
        return(dicionario_kv)

    @staticmethod
    def normalize_trafo_id(transformer_id: str) -> str:
        """Standardizes transformer ID normalization for lookups."""
        if not transformer_id or not isinstance(transformer_id, str):
            return str(transformer_id)
        return transformer_id[:-1] if transformer_id[-1].isalpha() else transformer_id

    @staticmethod
    def list_dsativ():
        return(list_dsativ)

    def dict_kv_pri():
        return(dicionario_kv_pri)

    def list_posse():
        return(list_posse)

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
                id_tr = Transformer.normalize_trafo_id(row[mapping_value])
                Transformer.sec_line_kv(transformer=id_tr,kv2=getattr(transformer_,"kv2"), tip_trafo=getattr(transformer_, "_Tip_Lig", None))
            if mapping_key == "sit_ativ" and row[mapping_value] == "DS":
                id_tr = Transformer.normalize_trafo_id(getattr(transformer_, f'_transformer'))
                list_dsativ.append(id_tr)
            if mapping_key == "posse" and row[mapping_value] != "PD":
                id_tr = Transformer.normalize_trafo_id(getattr(transformer_, f'_transformer'))
                list_posse.append(id_tr)

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
                    id_tr = Transformer.normalize_trafo_id(getattr(transformer_, f'_transformer'))
                    if "14339401A" in getattr(transformer_, f'_transformer'):
                        print(f"TRF_14339401A bus3_nodes RAW={param_value} -> MAPPED={function_(str(param_value))}")
                    Transformer.sec_phase_kv(id_tr,getattr(transformer_, f'_kv2'),getattr(transformer_, f'_bus2_nodes'),function_(str(param_value)),tip_trafo=getattr(transformer_,'_Tip_Lig'))
                    Transformer.register_available_phases(id_tr, function_(str(param_value)), tip_trafo=getattr(transformer_, '_Tip_Lig'))
                if mapping_key == 'kvas': #settings - limitar cargas BT (potencia atv do trafo): cria dicionário de trafos/potências
                    id_tr = Transformer.normalize_trafo_id(getattr(transformer_, f'_transformer'))
                    Transformer.dict_pot_tr(id_tr,function_(str(param_value)))
                if mapping_key == 'kv1':
                    dicionario_kv_pri[getattr(transformer_, f'_transformer')] = function_(str(param_value))
                if mapping_key == 'bus2_nodes':
                    id_tr = Transformer.normalize_trafo_id(getattr(transformer_, f'_transformer'))
                    Transformer.register_available_phases(id_tr, function_(str(param_value)), tip_trafo=getattr(transformer_, '_Tip_Lig'))
            else:
                setattr(transformer_, f"_{mapping_key}", row[mapping_value])

    @staticmethod
    def dict_kv():
        global dicionario_kv
        return dicionario_kv

    @staticmethod
    def dict_available_phases_data():
        global dict_available_phases
        return dict_available_phases

    @staticmethod
    def register_available_phases(transformer_id, bus2_nodes, tip_trafo=None):
        global dict_available_phases
        
        # 1. Parse current nodes
        current_nodes = [int(n) for n in bus2_nodes.split('.') if n.isdigit()]

        # 2. Accumulate ALL nodes seen for this transformer ID
        # We store the raw nodes set in the dict first to build the full picture
        trafo_data = dict_available_phases.setdefault(transformer_id, {'all_nodes': set(), 'tip_trafo': tip_trafo})
        if tip_trafo:
            trafo_data['tip_trafo'] = tip_trafo # Keep the last seen type (usually consistent)

        for n in current_nodes:
            trafo_data['all_nodes'].add(n)

        all_nodes = trafo_data['all_nodes']
        
        # 3. Identify roles
        neutral_node = 4 if 4 in all_nodes else (0 if 0 in all_nodes else None)
        hot_nodes = sorted([n for n in all_nodes if n in [1, 2, 3]])
        
        # 5. Generate options based on inferred connectivity
        available = {
            'single': [],
            'double': []
        }
        
        # Every hot node paired with neutral is a single-phase option
        if neutral_node is not None:
            for p in hot_nodes:
                available['single'].append(f"{p}.{neutral_node}")
        
        # Every pair of hot nodes is a double-phase option
        if len(hot_nodes) >= 2:
            import itertools
            # Standard pairs for 3-phase systems
            if len(hot_nodes) == 3:
                pairs = [(1, 2), (2, 3), (3, 1)]
            else:
                pairs = list(itertools.combinations(hot_nodes, 2))
                
            for p1, p2 in pairs:
                pair_str = f"{p1}.{p2}"
                available['double'].append(pair_str)
                if neutral_node is not None:
                    available['double'].append(f"{pair_str}.{neutral_node}")
        
        # 6. Store processed options
        trafo_data['single'] = sorted(list(set(available['single'])))
        trafo_data['double'] = sorted(list(set(available['double'])))
        
        # Diagnostic logging for troubleshooting phase mismatches
        # print(f"[DEBUG] Trafo {transformer_id}: nodes={all_nodes}, type={tip_trafo}, options={available}")

    @staticmethod
    def dict_phase_kv():
        global dict_phase_kv
        return dict_phase_kv

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
    def create_transformer_from_json(json_data: Any, dataframe: gpd.geodataframe.GeoDataFrame, pastadesaida: str = ""):
        global output
        list_reactors.clear()
        transformers = []
        transformer_config = json_data['elements']['Transformer']['UNTRMT']
        output = pastadesaida

        progress_bar = tqdm(dataframe.iterrows(), total=len(dataframe), desc="Transformer", unit=" transformers", ncols=100)
        for _, row in progress_bar:
            transformer_ = Transformer._create_transformer_from_row(transformer_config, row)
            transformers.append(transformer_)


        file_name = create_output_file(transformers, transformer_config["arquivo"], feeder=transformer_.feeder, output_folder=pastadesaida)

        return transformers, file_name

