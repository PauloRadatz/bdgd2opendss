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
import pandas as pd
import geopandas as gpd
from tqdm import tqdm
from bdgd2opendss.core.Settings import settings


from bdgd2opendss.model.Converter import convert_tten, convert_tfascon_bus, convert_tfascon_bus_prim, convert_tfascon_quant_fios, process_loadshape, process_loadshape2, convert_tfascon_conn_load, convert_tfascon_phases_load
from bdgd2opendss.core.Utils import create_output_file, create_output_folder,adequar_modelo_carga, get_cod_year_bdgd, elem_isolados, seq_eletrica
from bdgd2opendss.model.Transformer import Transformer #modificação 08/08
from bdgd2opendss.model.Circuit import Circuit
from bdgd2opendss.model.Count_days import return_day_type
import math

import numpy as np

from dataclasses import dataclass
df_energ_load = pd.DataFrame()

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
    _transformer: str = ""
    _flag_limitcarga: str = "" #settings - Flag Limitar potência de cargas BT(potência ativa do transformador)

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
    _energia_12: str = ''
    _energia_total: float = 0.0

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
    def transformer(self) -> str:
        return self._transformer

    @transformer.setter
    def transformer(self, value: str):
        self._transformer = value
    
    def adapting_string_variables_load(self): #TODO implementar as tensões de 254
        models = adequar_modelo_carga(settings.intAdequarModeloCarga)#settings adequar modelo de carga

        if "MT" not in self.entity:
            if settings.intAdequarTensaoCargasBT:#settings adequar tensão mínima das cargas BT
                self.vminpu = 0.92
            else:
                self.vminpu = settings.dblVPUMin 

            if self.phases == '1' and self.conn == 'Wye':
                kv = Transformer.sec_phase_kv(trload=self.transformer)      
            else:
                kv = Transformer.sec_line_kv(trload=self.transformer)

            return(kv,models)
        else:
            kv = seq_eletrica(key=self.bus1)

            if settings.intAdequarTensaoCargasMT:#settings adequar tensão mínima das cargas MT
                self.vminpu = 0.93
            else:
                self.vminpu = settings.dblVPUMin 

            return(kv,models)
    
    def limitar_potencia_cargasBT(self): #settings - Limitar potência de cargas BT(potência ativa do transformador)
        loadbtkw = Transformer.dict_pot_tr(trload=self.transformer)
        if float(self.kw) > loadbtkw*0.92:
            self._flag_limitcarga = '! Carga limitada'
            return(loadbtkw*0.92)
        else:
            return(self.kw)

    def full_string(self) -> str: 
        if self._energia_total == 0 or f'{self.entity}{self.load}' in elem_isolados():
            return("")
            
        if "MT" not in self.entity:
            if settings.intAdequarPotenciaCarga: #settings adequar potência das cargas BT(limitar a potência ativa do Transformador BT)
                self.kw = Load.limitar_potencia_cargasBT(self)

        try: #tratando erro numérico no cálculo de potência das cargas
            kw = (math.trunc(float(self.kw) * 10**6)/ 10**6)/2 #truncando de acordo com o geoperdas
        except:
            kw = float('nan')

        kv,models = Load.adapting_string_variables_load(self)
        # if flag_loadshape:
        #     daily = f"daily='{self.daily}_{self.tip_dia}'"
        # else:
        #     daily = ""

        return f'New \"Load.{self.entity}{self.load}_M1" bus1="{self.bus1}.{self.bus_nodes}" ' \
                f'phases={self.phases} conn={self.conn} model={models[0]} kv={kv:.9f} kw = {kw} '\
                f'pf={self.pf} status=variable vmaxpu={self.vmaxpu} vminpu={self.vminpu} ' \
                f'daily="{self.daily}_{self.tip_dia}" {self._flag_limitcarga} \n'\
                f'New \"Load.{self.entity}{self.load}_M2" bus1="{self.bus1}.{self.bus_nodes}" ' \
                f'phases={self.phases} conn={self.conn} model={models[1]} kv={kv:.9f} kw = {kw} '\
                f'pf={self.pf} status=variable vmaxpu={self.vmaxpu} vminpu={self.vminpu} ' \
                f'daily="{self.daily}_{self.tip_dia}" {self._flag_limitcarga}'
                
            
    def __repr__(self):
        if self._energia_total == 0 or f'{self.entity}{self.load}' in elem_isolados():
            return("")
            
        if "MT" not in self.entity:
            # if self.transformer in Transformer.list_dsativ() or self.transformer not in Transformer.dict_kv().keys(): #remove as cargas desativadas
            #     return("")
            if settings.intAdequarPotenciaCarga: #settings adequar potência das cargas BT(limitar a potência ativa do Transformador BT)
                self.kw = Load.limitar_potencia_cargasBT(self)

        try: #tratando erro numérico no cálculo de potência das cargas
            kw = (math.trunc(float(self.kw) * 10**6)/ 10**6)/2 #truncando de acordo com o geoperdas
        except:
            kw = float('nan')

        kv,models = Load.adapting_string_variables_load(self)
        return f'New \"Load.{self.entity}{self.load}_M1" bus1="{self.bus1}.{self.bus_nodes}" ' \
                f'phases={self.phases} conn={self.conn} model={models[0]} kv={kv:.9f} kw = {kw} '\
                f'pf={self.pf} status=variable vmaxpu={self.vmaxpu} vminpu={self.vminpu} ' \
                f'daily="{self.daily}_{self.tip_dia}" {self._flag_limitcarga} \n'\
                f'New \"Load.{self.entity}{self.load}_M2" bus1="{self.bus1}.{self.bus_nodes}" ' \
                f'phases={self.phases} conn={self.conn} model={models[1]} kv={kv:.9f} kw = {kw} '\
                f'pf={self.pf} status=variable vmaxpu={self.vmaxpu} vminpu={self.vminpu} ' \
                f'daily="{self.daily}_{self.tip_dia}" {self._flag_limitcarga}'

    # @jit(nopython=True)
    def calculate_kw(self, df, tip_dia="", mes="01"):
        #global flag_loadshape 
        global df_energ_load
        df = df.copy()
        df["prop_pot_tipdia_mes"] = None 
        #print('aqui')

        try:
            for index, row in df.iterrows():
                df.loc[index, "prop_pot_tipdia_mes"] = row["prop"]*return_day_type(index,mes) 
                

            prop_pot_mens_mes = df["prop_pot_tipdia_mes"][tip_dia]/(df["prop_pot_tipdia_mes"].sum()) #Tirar aqui o propenermensal(TIPDIA)(MES) para cada carga

            pot_atv_media = df["soma_pot"][tip_dia]/24
            pot_atv_max = max(df["pot_atv"][tip_dia])
            fc = pot_atv_media/pot_atv_max #tirar aqui o fcDU/fcDO/fcSA para cada carga
            kw = getattr(self, f'energia_{mes}')*(prop_pot_mens_mes)/(return_day_type(tip_dia, mes)*24*fc)#kw tipo dia (DU/SA/DO)
            energia = getattr(self, f'energia_{mes}')
            if self._energia_total != 0: #não cria df de cargas com energia zerada
                Load.create_df_loads(self,tip_dia,mes,df['COD_ID'][tip_dia],prop_pot_mens_mes,fc,kw,energia) #cria o dataframe para usar no cálculo das perdas técnicas
            return (kw)

        except KeyError: #TODO implementar uma curva default quando não houver loadshape na BDGD 
            
            #flag_loadshape = False
            print("There's no corresponding loadshape for this load")
            return (float('nan'))

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
        lista_energia = []
        for mapping_key, mapping_value in value.items():
            setattr(load_, f"_{mapping_key}", row[mapping_value])
            if 'energia' in mapping_key:
                lista_energia.append(row[mapping_value])
                if mapping_key == 'energia_12':
                    setattr(load_, f"_energia_total", sum(lista_energia))


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
    def _create_output_load_files(dict_loads_tip_day: dict, tip_day: str, feeder: str, name: str, pastadesaida: str = ""):

        load_file_names = []
        load_lists= []

        for key, value in dict_loads_tip_day.items():
            load_file_names.append(f'{name[:8]}_{tip_day}{key}')
            load_lists.append(value)

        return create_output_file(object_lists=load_lists,file_name=name, file_names= load_file_names, feeder=feeder, output_folder=pastadesaida)

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
        #print('aqui')
        # dataframe['soma_pot'] = np.sum(np.array(dataframe['pot_atv'].tolist()), axis=1)
        dataframe['soma_pot'] = dataframe['pot_atv'].apply(np.sum)
        pot_classe = dataframe['soma_pot'].sum()
        dataframe['prop'] = dataframe['soma_pot'] / pot_classe
        return dataframe


    @staticmethod
    def _create_load_from_row(load_config, row, entity, id):

        load_ = Load()
        if entity != "PIP":
            setattr(load_, "_entity", f'{entity[2] + entity[3]}_')
        else:
            setattr(load_, "_entity", "BT_IP")

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
    def create_load_from_json(json_data: Any, dataframe: gpd.geodataframe.GeoDataFrame,crv_dataframe: gpd.geodataframe.GeoDataFrame, entity: str, pastadesaida: str = ""):
    #def create_load_from_json(json_data: Any, dataframe: gpd.geodataframe.GeoDataFrame,crv_dataframe: gpd.geodataframe.GeoDataFrame, entity: str, kVbaseObj: Any, pastadesaida: str = ""):
        
        # global _kVbase_GLOBAL #TODO Verificar com o Ezequiel
        # _kVbase_GLOBAL = kVbaseObj.MV_kVbase

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
                        new_load.kw = new_load.calculate_kw(df=crv_dataframe_aux, tip_dia=i, mes=mes) #TODO observar aqui o problema dos loadshapes

                        if i=="DU":
                            DU_meses[mes].append(new_load)
                        elif i =="SA":
                            SA_meses[mes].append(new_load)
                        elif i =="DO":
                            DO_meses[mes].append(new_load)


            progress_bar.set_description(f"Processing load {entity} {_ + 1}")
        global df_energ_load
        df_energ_load['CodDist'] = get_cod_year_bdgd()
        file_name = Load._create_output_load_files(DU_meses, "DU", name= load_config["arquivo"], feeder=load_.feeder, pastadesaida=pastadesaida)
        Load._create_output_load_files(SA_meses, "SA", name= load_config["arquivo"], feeder=load_.feeder, pastadesaida=pastadesaida)
        Load._create_output_load_files(DO_meses, "DO", name= load_config["arquivo"], feeder=load_.feeder, pastadesaida=pastadesaida)

        return DU_meses, file_name
        #return load_, file_name

    def create_df_loads(self,tip_dia,mes,crvcarga,prop,fc,kw,energia):
        global df_energ_load
        if df_energ_load.empty:
            colunas = []
            dias = []
            colunas_ene = []
            tipo_dia = ['DU','SA','DO']
            for dia in tipo_dia:
                for month in range(1,13):
                    colunas.append('PropEnerMens'+f'{dia}{month:02d}')
                    colunas.append('DemMax'+f'{dia}{month:02d}_kW') 
                    dias.append('DiasMes'+f'{dia}{month:02d}')     
            for month in range(1,13):
                    colunas_ene.append('EnerMedid'+f'{month:02d}_MWh')
            columns = ['CodDist','CodConsBT','TipCrvaCarga','CodAlim','CodTrafo','fcDU','fcSA','fcDO'] + colunas_ene + colunas[::2] + dias + colunas[1::2]
            
            df_energ_load = pd.DataFrame(columns=columns)
            df_energ_load['CodAlim'] = self.feeder 
             #df_energ_load.set_index('CodConsBT', inplace=True)
        else:
            ...
        #TODO corrigir aqui
        if self.entity == 'BT_IP':  
            df_energ_load.at[self.load, f'CodConsBT'] = 'IP' + self.load
        else:
            df_energ_load.at[self.load, f'CodConsBT'] = self.load
        df_energ_load.at[self.load, f'PropEnerMens{tip_dia}{mes}'] = prop
        df_energ_load.at[self.load, f'EnerMedid{mes}_MWh'] = float(energia)/1000
        df_energ_load.at[self.load, f'DemMax{tip_dia}{mes}_kW'] = kw
        df_energ_load.at[self.load, f'DiasMes{tip_dia}{mes}'] = return_day_type(tip_dia,str(mes))
        df_energ_load.at[self.load, f'fc{tip_dia}'] = fc
        df_energ_load.at[self.load, 'TipCrvaCarga'] = crvcarga
        df_energ_load.at[self.load, 'CodAlim'] = self.feeder
        df_energ_load.at[self.load, 'CodTrafo'] = self.transformer

    def export_df_loads(output,feeder,data_bdgd,cod_bdgd):
        global df_energ_load
        df_energ_load['CodDist'] = data_bdgd + cod_bdgd
        df_energ_mtload = df_energ_load[df_energ_load['TipCrvaCarga'].str.contains('MT')].drop('CodTrafo', axis=1)
        df_energ_mtload.rename(columns={'CodConsBT': 'CodConsMT'}, inplace=True)
        df_energ_btload = df_energ_load[~df_energ_load['TipCrvaCarga'].str.contains('MT')]
        output_folder = create_output_folder(feeder=feeder, output_folder=output)
        path_file_bt = output_folder + r"/AuxCargaBTNT" + f"_{feeder}.csv"
        path_file_mt = output_folder + r"/AuxCargaMTNT" + f"_{feeder}.csv"
        df_energ_btload.to_csv(path_file_bt,encoding='utf-8', decimal='.', index=False)
        df_energ_mtload.to_csv(path_file_mt,encoding='utf-8', decimal='.',index=False)
        return(print('Tabela de perdas técnicas criada'))
        