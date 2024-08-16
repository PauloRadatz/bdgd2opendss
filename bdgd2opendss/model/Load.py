# -*- encoding: utf-8 -*-
"""
 * Project Name: Load.py
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
# from numba import jit
import pandas
import geopandas as gpd
from tqdm import tqdm


from bdgd2opendss.model.Converter import convert_tten, convert_tfascon_phases, convert_tfascon_bus, convert_tfascon_bus_prim, convert_tfascon_quant_fios, convert_tfascon_conn, process_loadshape, process_loadshape2, qt_tipdia_mes, convert_tfascon_conn_load, convert_tfascon_phases_load
from bdgd2opendss.core.Utils import create_output_file
from bdgd2opendss.model.Transformer import dicionario_kv #modificação 08/08

import numpy as np

from dataclasses import dataclass

dicionario = {}

@dataclass
class Load:

    _feeder: str = ""
    _pf: float = 0.92
    _vminpu: float = 0.93
    _vmaxpu: float = 1.50


    _bus1: str = ""
    _load: str = ""
    _daily: str = ""
    _phases: str = ""
    _conn: str = ""
    _bus_nodes: str = ""
    _kv: str = ""
    _kw: str = ""
    _GD_setter: str = ""

    _tip_dia: str = ""
    _load_DO: str = ""
    _load_DU: str = ""
    _load_SA: str = ""

    _entity: str =''

    _energia_01: str = ''
    _energia_02: str = ''
    _energia_03: str = ''
    _energia_04: str = ''
    _energia_05: str = ''
    _energia_06: str = ''
    _energia_07: str = ''
    _energia_08: str = ''
    _energia_09: str = ''
    _energia_10: str = ''
    _energia_11: str = ''

    @property
    def feeder(self):
        return self._feeder

    @feeder.setter
    def feeder(self, value: float):
        self._feeder = value

    @property
    def pf(self):
        return self._pf

    @pf.setter
    def pf(self, value: float):
        self._pf = value

    @property
    def vminpu(self):
        return self._vminpu

    @vminpu.setter
    def vminpu(self, value: float):
        self._vminpu = value

    @property
    def vmaxpu(self):
        return self._vmaxpu

    @vmaxpu.setter
    def vmaxpu(self, value: float):
        self._vmaxpu = value

    @property
    def bus1(self):
        return self._bus1

    @bus1.setter
    def bus1(self, value: str):
        self._bus1 = value

    @property
    def load(self):
        return self._load

    @load.setter
    def load(self, value: str):
        self._load = value

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value: str):
        self._id = value

    @property
    def daily(self):
        return self._daily

    @daily.setter
    def daily(self, value: str):
        self._daily = value

    @property
    def phases(self):
        return self._phases

    @phases.setter
    def phases(self, value: str):
        self._phases = value

    @property
    def conn(self):
        return self._conn

    @conn.setter
    def conn(self, value: str):
        self._conn = value

    @property
    def bus_nodes(self):
        return self._bus_nodes

    @bus_nodes.setter
    def bus_nodes(self, value: str):
        self._bus_nodes = value

    @property
    def kv(self):
        return self._kv

    @kv.setter
    def kv(self, value: str):
        self._kv = value

    @property
    def kw(self):
        return self._kw

    @kw.setter
    def kw(self, value: str):
        self._kw = value

    @property
    def entity(self):
        return self._entity

    @entity.setter
    def entity(self, value: str):
        self._entity = value

    @property
    def tip_dia(self):
        return self._tip_dia

    @tip_dia.setter
    def tip_dia(self, value: float):
        self._tip_dia = value


    @property
    def load_DO(self):
        return self._load_DO

    @load_DO.setter
    def load_DO(self, value: float):
        self._load_DO = value

    @property
    def load_SA(self):
        return self._load_SA

    @load_SA.setter
    def load_SA(self, value: float):
        self._load_SA = value

    @property
    def load_DU(self):
        return self._load_DU

    @load_DU.setter
    def load_DU(self, value: float):
        self._load_DU = value

    @property
    def energia_01(self) -> str:
        return self._energia_01

    @energia_01.setter
    def energia_01(self, value: str):
        self._energia_01 = value

    @property
    def energia_02(self) -> str:
        return self._energia_02

    @energia_02.setter
    def energia_02(self, value: str):
        self._energia_02 = value

    @property
    def energia_03(self) -> str:
        return self._energia_03

    @energia_03.setter
    def energia_03(self, value: str):
        self._energia_03 = value

    @property
    def energia_04(self) -> str:
        return self._energia_04

    @energia_04.setter
    def energia_04(self, value: str):
        self._energia_04 = value

    @property
    def energia_05(self) -> str:
        return self._energia_05

    @energia_05.setter
    def energia_05(self, value: str):
        self._energia_05 = value

    @property
    def energia_06(self) -> str:
        return self._energia_06

    @energia_06.setter
    def energia_06(self, value: str):
        self._energia_06 = value

    @property
    def energia_07(self) -> str:
        return self._energia_07

    @energia_07.setter
    def energia_07(self, value: str):
        self._energia_07 = value

    @property
    def energia_08(self) -> str:
        return self._energia_08

    @energia_08.setter
    def energia_08(self, value: str):
        self._energia_08 = value

    @property
    def energia_09(self) -> str:
        return self._energia_09

    @energia_09.setter
    def energia_09(self, value: str):
        self._energia_09 = value

    @property
    def energia_10(self) -> str:
        return self._energia_10

    @energia_10.setter
    def energia_10(self, value: str):
        self._energia_10 = value

    @property
    def energia_11(self) -> str:
        return self._energia_11

    @energia_11.setter
    def energia_11(self, value: str):
        self._energia_11 = value

    @property
    def energia_12(self) -> str:
        return self._energia_12

    @energia_12.setter
    def energia_12(self, value: str):
        self._energia_12 = value

    @property
    def GD_setter(self) -> str:
        return self._GD_setter

    @GD_setter.setter
    def GD_setter(self, value: str):
        self._GD_setter = value

    def full_string(self) -> str: #cargas de 2 ou 3 fases devem ter tensão de linha

        if int(self.phases) == 1 and (float(self.kv) == 0.240):
            return f'New \"Load.{self.entity}_{self.load}_{self.id}_M1" bus1="{self.bus1}.{self.bus_nodes}" ' \
                f'phases={self.phases} conn={self.conn} model=1 kv={float(self.kv)/2} kw = {self.kw/2:.7f} '\
                f'pf={self.pf} status=variable vmaxpu={self.vmaxpu} vminpu={self.vminpu} ' \
                f'daily="{self.daily}_{self.tip_dia}" \n'\
                f'New \"Load.{self.entity}_{self.load}_{self.id}_M2" bus1="{self.bus1}.{self.bus_nodes}" ' \
                f'phases={self.phases} conn={self.conn} model=2 kv={float(self.kv)/2} kw = {self.kw/2:.7f} '\
                f'pf={self.pf} status=variable vmaxpu={self.vmaxpu} vminpu={self.vminpu} ' \
                f'daily="{self.daily}_{self.tip_dia}"\n '
        elif self.conn == "Delta" or int(self.phases) > 1:
            return f'New \"Load.{self.entity}_{self.load}_{self.id}_M1" bus1="{self.bus1}.{self.bus_nodes}" ' \
                f'phases={self.phases} conn={self.conn} model=1 kv={self.kv} kw = {self.kw/2:.7f} '\
                f'pf={self.pf} status=variable vmaxpu={self.vmaxpu} vminpu={self.vminpu} ' \
                f'daily="{self.daily}_{self.tip_dia}" \n'\
                f'New \"Load.{self.entity}_{self.load}_{self.id}_M2" bus1="{self.bus1}.{self.bus_nodes}" ' \
                f'phases={self.phases} conn={self.conn} model=2 kv={self.kv} kw = {self.kw/2:.7f} '\
                f'pf={self.pf} status=variable vmaxpu={self.vmaxpu} vminpu={self.vminpu} ' \
                f'daily="{self.daily}_{self.tip_dia}"\n '
        else:
            return f'New \"Load.{self.entity}_{self.load}_{self.id}_M1" bus1="{self.bus1}.{self.bus_nodes}" ' \
                f'phases={self.phases} conn={self.conn} model=1 kv={float(self.kv)/np.sqrt(3):.3f} kw = {self.kw/2:.7f} '\
                f'pf={self.pf} status=variable vmaxpu={self.vmaxpu} vminpu={self.vminpu} ' \
                f'daily="{self.daily}_{self.tip_dia}" \n'\
                f'New \"Load.{self.entity}_{self.load}_{self.id}_M2" bus1="{self.bus1}.{self.bus_nodes}" ' \
                f'phases={self.phases} conn={self.conn} model=2 kv={float(self.kv)/np.sqrt(3):.3f} kw = {self.kw/2:.7f} '\
                f'pf={self.pf} status=variable vmaxpu={self.vmaxpu} vminpu={self.vminpu} ' \
                f'daily="{self.daily}_{self.tip_dia}"\n '

    def __repr__(self):

        if int(self.phases) == 1 and (float(self.kv) == 0.240):
            return f'New \"Load.{self.entity}_{self.load}_{self.id}_M1" bus1="{self.bus1}.{self.bus_nodes}" ' \
                f'phases={self.phases} conn={self.conn} model=1 kv={float(self.kv)/2} kw = {self.kw/2:.7f} '\
                f'pf={self.pf} status=variable vmaxpu={self.vmaxpu} vminpu={self.vminpu} ' \
                f'daily="{self.daily}_{self.tip_dia}" \n'\
                f'New \"Load.{self.entity}_{self.load}_{self.id}_M2" bus1="{self.bus1}.{self.bus_nodes}" ' \
                f'phases={self.phases} conn={self.conn} model=2 kv={float(self.kv)/2} kw = {self.kw/2:.7f} '\
                f'pf={self.pf} status=variable vmaxpu={self.vmaxpu} vminpu={self.vminpu} ' \
                f'daily="{self.daily}_{self.tip_dia}"\n '
        elif self.conn == "Delta" or int(self.phases) > 1:
            return f'New \"Load.{self.entity}_{self.load}_{self.id}_M1" bus1="{self.bus1}.{self.bus_nodes}" ' \
                f'phases={self.phases} conn={self.conn} model=1 kv={self.kv} kw = {self.kw/2:.7f} '\
                f'pf={self.pf} status=variable vmaxpu={self.vmaxpu} vminpu={self.vminpu} ' \
                f'daily="{self.daily}_{self.tip_dia}" \n'\
                f'New \"Load.{self.entity}_{self.load}_{self.id}_M2" bus1="{self.bus1}.{self.bus_nodes}" ' \
                f'phases={self.phases} conn={self.conn} model=2 kv={self.kv} kw = {self.kw/2:.7f} '\
                f'pf={self.pf} status=variable vmaxpu={self.vmaxpu} vminpu={self.vminpu} ' \
                f'daily="{self.daily}_{self.tip_dia}"\n '
        else:
            return f'New \"Load.{self.entity}_{self.load}_{self.id}_M1" bus1="{self.bus1}.{self.bus_nodes}" ' \
                f'phases={self.phases} conn={self.conn} model=1 kv={float(self.kv)/np.sqrt(3):.3f} kw = {self.kw/2:.7f} '\
                f'pf={self.pf} status=variable vmaxpu={self.vmaxpu} vminpu={self.vminpu} ' \
                f'daily="{self.daily}_{self.tip_dia}" \n'\
                f'New \"Load.{self.entity}_{self.load}_{self.id}_M2" bus1="{self.bus1}.{self.bus_nodes}" ' \
                f'phases={self.phases} conn={self.conn} model=2 kv={float(self.kv)/np.sqrt(3):.3f} kw = {self.kw/2:.7f} '\
                f'pf={self.pf} status=variable vmaxpu={self.vmaxpu} vminpu={self.vminpu} ' \
                f'daily="{self.daily}_{self.tip_dia}"\n '

    # @jit(nopython=True)
    def calculate_kw(self, df, tip_dia="", mes="01"):
        df = df.copy()
        df["prop_pot_tipdia_mes"] = None #modificação feita por Mozart - 13/06 Às 12:55h

        try:
            for index, row in df.iterrows():
                df.loc[index, "prop_pot_tipdia_mes"] = row["prop"]*qt_tipdia_mes(index,mes)


            prop_pot_mens_mes = df["prop_pot_tipdia_mes"][tip_dia]/(df["prop_pot_tipdia_mes"].sum())

            pot_atv_media = df["soma_pot"][tip_dia]/24
            pot_atv_max = max(df["pot_atv"][tip_dia])
            fc = pot_atv_media/pot_atv_max

            return (getattr(self, f'energia_{mes}')*(prop_pot_mens_mes*1000)/(qt_tipdia_mes(tip_dia, mes)*24*fc))/1000

        except KeyError:

            print("There's no corresponding loadshape for this load")

    @staticmethod
    def _process_static(load_, value):
        """
        Static method to process the static configuration for a load object.

        Args:
            load_ (object): A load object being updated.
            value (dict): A dictionary containing the static configuration.

        This method processes the static configuration by iterating through the
        key-value pairs of the 'value' dictionary and directly setting the
        corresponding attribute on the load object with the static value.
        """
        for static_key, static_value in value.items():
            setattr(load_, f"_{static_key}", static_value)


    @staticmethod
    def _process_direct_mapping(load_, value, row):
        """
        Static method to process the direct mapping configuration for a load object.

        Args:
            load_ (object): A load object being updated.
            value (dict): A dictionary containing the direct mapping configuration.
            row (pd.Series): A row from the GeoDataFrame containing load-related data.

        This method processes the direct mapping configuration by iterating through the
        key-value pairs of the 'value' dictionary and directly setting the corresponding
        attribute on the load object using the value from the row.
        """
        for mapping_key, mapping_value in value.items():
            setattr(load_, f"_{mapping_key}", row[mapping_value])
            if mapping_key == "GD_setter" and "G" in row[mapping_value]:
                dicionario[row[mapping_value]] = getattr(load_,"bus_nodes")
            if mapping_key == "Trafo":
                try:
                    setattr(load_, f"_kv", dicionario_kv[row[mapping_value]])
                except KeyError:
                    continue

    @staticmethod
    def _process_indirect_mapping(load_, value, row):
        """
        Static method to process the indirect mapping configuration for a load object.

        Args:
            load_ (object): A load object being updated.
            value (dict): A dictionary containing the indirect mapping configuration.
            row (pd.Series): A row from the GeoDataFrame containing load-related data.

        This method processes the indirect mapping configuration by iterating through the
        key-value pairs of the 'value' dictionary. If the value is a list, it treats the
        first element as a parameter name and the second element as a function name. The
        method then retrieves the parameter value from the row and calls the specified
        function with that parameter value. The result is then set as an attribute on the
        load object.

        If the value is not a list, the method directly sets the corresponding attribute on
        the load object using the value from the row.
        """
        for mapping_key, mapping_value in value.items():
            if isinstance(mapping_value, list):
                param_name, function_name = mapping_value
                function_ = globals()[function_name]
                param_value = row[param_name]
                setattr(load_, f"_{mapping_key}", function_(str(param_value)))
            else:
                setattr(load_, f"_{mapping_key}", row[mapping_value])


    @staticmethod
    def _process_calculated(load_, value, row):
        """
        Static method to process the calculated mapping configuration for a load object.

        Args:
            load_ (object): A load object being updated.
            value (dict): A dictionary containing the direct mapping configuration.
            row (pd.Series): A row from the GeoDataFrame containing load-related data.

        This method processes the direct mapping configuration by iterating through the
        key-value pairs of the 'value' dictionary and directly setting the corresponding
        attribute on the load object using the value from the row.
        """
        for mapping_key, mapping_value in value.items():

            expression = ""
            for item in mapping_value:
                if isinstance(item, str) and any(char.isalpha() for char in item):

                    expression = f'{expression} {row[item]}'
                else:
                    expression = f'{expression}{item}'

            param_value = eval(expression)

            setattr(load_, f"_{mapping_key}", param_value)


    @staticmethod
    def _create_output_load_files(dict_loads_tip_day: dict, tip_day: str, feeder: str, name: str):

        load_file_names = []
        load_lists= []

        for key, value in dict_loads_tip_day.items():

            load_file_names.append(f'{name}_{tip_day}{key}')
            load_lists.append(value)


        return create_output_file(object_lists=load_lists, file_names= load_file_names, feeder=feeder)

    @staticmethod
    def compute_pre_kw(dataframe: gpd.geodataframe.GeoDataFrame):

        dataframe['loadshape'] = None
        dataframe['pot_atv'] = None

        for i in range(0,len(dataframe)):
            # pot_atv_normal, pot_atv = process_loadshape(dataframe.filter(regex='^POT').loc[i,:].to_list())        # manda uma lista com os 96 valores de uma carga apenas
            pot_atv_normal, pot_atv = process_loadshape(dataframe.filter(regex='^POT').loc[i,:].to_list())        # manda uma lista com os 96 valores de uma carga apenas


            dataframe.at[i,'loadshape'] = pot_atv_normal
            dataframe.at[i,'pot_atv'] = pot_atv

        dataframe = dataframe.loc[:, ['COD_ID', 'TIP_DIA', 'loadshape', 'pot_atv']]
        dataframe.set_index('TIP_DIA', inplace=True)

        dataframe['soma_pot'] = np.sum(np.array(dataframe['pot_atv'].tolist()), axis=1)
        pot_classe = dataframe['soma_pot'].sum()
        dataframe['prop'] = dataframe['soma_pot'] / pot_classe
        return dataframe


    @staticmethod
    def _create_load_from_row(load_config, row, entity, id):

        load_ = Load()
        if entity != "PIP":
            setattr(load_, "_entity", entity[2] + entity[3])
        else:
            setattr(load_, "_entity", "IP")

        setattr(load_, "_id", id)

        for key, value in load_config.items():
            if key == "calculated":
                load_._process_calculated(load_, value, row)
            elif key == "indirect_mapping":
                load_._process_indirect_mapping(load_, value,row)
            elif key == "direct_mapping":
                load_._process_direct_mapping(load_, value,row)
            # elif key == "indirect_mapping":
            #     load_._process_indirect_mapping(load_, value,row)
            elif key == "static":
                load_._process_static(load_, value)
        return load_

    @staticmethod
    def create_load_from_json(json_data: Any, dataframe: gpd.geodataframe.GeoDataFrame,crv_dataframe: gpd.geodataframe.GeoDataFrame, entity: str):

        DU_meses = {"01": [],"02": [],"03": [],"04": [],"05": [],"06": [],"07": [],"08": [],"09": [],"10": [],"11": [],"12": []}
        DO_meses = {"01": [],"02": [],"03": [],"04": [],"05": [],"06": [],"07": [],"08": [],"09": [],"10": [],"11": [],"12": []}
        SA_meses = {"01": [],"02": [],"03": [],"04": [],"05": [],"06": [],"07": [],"08": [],"09": [],"10": [],"11": [],"12": []}

        meses = [f"{mes:02d}" for mes in range(1, 13)]

        load_config = json_data['elements']['Load'][entity]
        interactive = load_config.get('interactive')
        crv_dataframe = Load.compute_pre_kw(crv_dataframe)
        # dataframe = dataframe.head(200)

        progress_bar = tqdm(dataframe.iterrows(), total=len(dataframe), desc="Load", unit=" loads", ncols=100)
        for _, row in progress_bar:
            load_ = Load._create_load_from_row(load_config, row, entity, _)
            crv_dataframe_aux = crv_dataframe[crv_dataframe['COD_ID'] == f'{load_.daily}']

            if interactive is not None: #parametro_iteravel, objeto
                for i in interactive['tip_dias']:

                    for mes in meses:
                        new_load = copy.deepcopy(load_)
                        new_load.tip_dia = i
                        new_load.kw = new_load.calculate_kw(df=crv_dataframe_aux, tip_dia=i, mes=mes)

                        if i=="DU":
                            DU_meses[mes].append(new_load)
                        elif i =="SA":
                            SA_meses[mes].append(new_load)
                        elif i =="DO":
                            DO_meses[mes].append(new_load)



            progress_bar.set_description(f"Processing load {entity} {_ + 1}")

        file_name = Load._create_output_load_files(DU_meses, "DU", name= load_config["arquivo"], feeder=load_.feeder)
        Load._create_output_load_files(SA_meses, "SA", name= load_config["arquivo"], feeder=load_.feeder)
        Load._create_output_load_files(DO_meses, "DO", name= load_config["arquivo"], feeder=load_.feeder)

        return DU_meses, file_name
