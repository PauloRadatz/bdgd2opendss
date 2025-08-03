# -*- encoding: utf-8 -*-

import json
import os.path
import pathlib
from typing import Any, Optional
import re
import sys
import networkx as nx
import geopandas as gpd
import pandas as pd
from bdgd2opendss.core.Settings import settings
import logging

cod_year_bdgd = []
tr_vazios = []
sufixo_config = ""
lista_isolados = []
tensao_dict = {}
substation = ""

def log_erros(df_isolados:Optional[pd.DataFrame]=None,feeder:Optional[str]=None,output_directory: Optional[str] = None, ctmt:Optional[str] = None):
    logger = logging.getLogger(f'elementos_isolados_{get_cod_year_bdgd(typ="cod")}')
    if not logger.hasHandlers():
        if output_directory == None: #caso não exista o caminho para uma pasta de saída
            output_directory = create_output_folder(feeder=ctmt,output_folder=output_directory)
            #output_directory = os.path.dirname(file_path)
        file_path = os.path.join(output_directory, f'elementos_isolados_{get_cod_year_bdgd(typ="cod")}.log')
        logging.basicConfig(
            level=logging.INFO,  # Configura o nível mínimo de log (neste caso, INFO)
            format='%(levelname)s - %(message)s',  # Formato sem data/hora, apenas o nível e a mensagem
            filename = file_path,
            filemode='w'  # Sobrescrever o arquivo de log (use 'a' para adicionar ao invés de sobrescrever)
            )
    if ctmt is None: 
        for _,row in df_isolados.iterrows(): 
            logger.info(f'Elemento isolado - COD_ID:{row["COD_ID"]} - TIPO:{row["ELEM"]} - CTMT:{row["CTMT"]} - PAC1:{row["PAC_1"]} - PAC2:{row["PAC_2"]}')
    else:
        logger.info(f'O alimentador {feeder} não tem conexão com a barra incial {ctmt}')
def load_json(json_file: str = "bdgd2dss.json"):
    """Carrega os dados de um arquivo JSON e retorna um objeto Python.

    :param json_file: O nome do arquivo JSON (padrão: "bdgd2dss.json").
    :return: Um objeto Python contendo os dados do arquivo JSON.
    """
    print(f"Carregando o arquivo JSON: {json_file}")
    json_path = pathlib.Path(json_file)

    try:
        with json_path.open() as jf:
            data = json.load(jf)
    except FileNotFoundError:
        print(
            f"Arquivo {json_file} não encontrado. O arquivo deve estar no mesmo diretório do arquivo .py do seu projeto.")
        return None
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar o arquivo JSON: {e}")
        return None

    return data


def merge_entities_tables(dataframe1: gpd.geodataframe.GeoDataFrame, dataframe2: gpd.geodataframe.GeoDataFrame):
    """
    Merge two GeoDataFrames of entities based on their indices and handle duplicated columns.

    It's necessary when the element needs more of one table of the BDGD.

    Parameters:
    dataframe1 (gpd.geodataframe.GeoDataFrame): The first GeoDataFrame (entity table) to be merged.
    dataframe2 (gpd.geodataframe.GeoDataFrame): The second GeoDataFrame (entity table) to be merged.

    Returns:
    gpd.geodataframe.GeoDataFrame: The merged GeoDataFrame with duplicated columns removed.

    """

    merged_dfs = dataframe2.join(dataframe1, lsuffix='_left')
    duplicated_columns = [col for col in merged_dfs.columns if '_left' in col]
    merged_dfs.drop(columns=duplicated_columns, inplace=True)

    return merged_dfs


def inner_entities_tables(entity1_df, enetity2_df, left_column: str = "", right_column: str = ""):
    """
    Merge two entities's DataFrames using an inner join and process the resulting DataFrame.

    This function takes two DataFrames, 'entity1_df' and 'entity2_df', and merges them
    using an inner join on the 'UN_RE' column of 'entity1_df' and the 'COD_ID' column
    of 'entity2_df'. The resulting merged DataFrame is then processed to remove
    redundant columns and rename columns as necessary.

    Parameters:
    - entity1_df (pandas.DataFrame): The first DataFrame to be merged. It belongs to an entity A.
    - entity2_df (pandas.DataFrame): The second DataFrame to be merged. It belongs to an entity B.

    Returns:
    - pandas.DataFrame: A new DataFrame

    Example:
    entity1_df:
        UN_RE  POT_NOM_x  PAC1_x
        A     Value1     Value2
        B     Value3     Value4

    entity2_df:
        COD_ID  POT_NOM_y  PAC3_y
        A      Value5     Value6
        C      Value7     Value8

    Resulting DataFrame after calling inner_entities_tables(entity1_df, entity2_df):
        UN_RE  POT_NOM  CPOT_NOM  COD_ID  PAC3
        A      Value1    Value2       A     Value6

    Note:
    - Columns with '_x' suffix are dropped.
    - Columns with '_y' suffix have the suffix removed.

    """
    merged_dfs = pd.merge(entity1_df, enetity2_df, left_on=left_column, right_on=right_column, how='inner')
    # if left_column == 'UN_RE':
    #     merged_dfs = pd.merge(entity1_df, enetity2_df, left_on=left_column, right_on=right_column, how='inner')
    # else:
    #     merged_dfs = pd.merge(entity1_df, enetity2_df, left_on=left_column, right_on=right_column, how='right') #Pegar UNI_TR_MT que não tenham EQTRMT (geram linhas e cargas isoladas)
    #     counter = 0
    #     for value in merged_dfs['UNI_TR_MT']:
    #         if pd.isna(value): #remove transformadores desativados que não estão com SIT_ATIV = DS na BDGD.
    #             merged_dfs.loc[counter,"SIT_ATIV"] = "DS"
    #         else:
    #             merged_dfs.loc[counter,"SIT_ATIV"] = "AT"
    #         counter += 1 
    #     merged_dfs['POT_NOM'] = merged_dfs["POT_NOM"].fillna(0).astype(int) 
    #     merged_dfs['TEN_PRI'] = merged_dfs["TEN_PRI"].fillna(0).astype(int)
    for column in merged_dfs.columns:
        if column.endswith('_x'):
            merged_dfs.drop(columns=column, inplace=True)
        elif column.endswith('_y'):
            new_column_name = column[:-2]  # Remove the '_df2' suffix
            merged_dfs.rename(columns={column: new_column_name}, inplace=True)

    return merged_dfs


def create_output_file(object_list=[], file_name="", object_lists="", file_names="", output_folder="", feeder=""): 
    """Create an dss_models_output file and write data from a list of objects.

    Parameters:
    - object_list (list): List of objects to be written to the file.Ex: line or transformer objects.
    - file_name (str): Name of the dss_models_output file. Ex: transformers.txt or lines.txt

    Creates an dss_models_output file in the 'dss_models_output' directory and writes OpenDSS commands from the list,
    separated by newline characters. If any error occurs, it will be displayed.

    """
    output_directory = create_output_folder(feeder=feeder,output_folder=output_folder)
    
    if object_lists != "":
        if file_name == 'CargasBT_IP':
            k = 'a' #anexação de arquivo
            file_name = ""
        else:
            k = 'w' #sobre-escrevendo o arquivo
            file_name = ""
        for object_list, file_name in zip(object_lists, file_names):
            path = os.path.join(output_directory, f'{file_name}_{get_cod_year_bdgd(typ="yearcod")}_{feeder}_{get_configuration()}.dss')

            
            with open(path, k) as file:
                for string in object_list:
                    try:
                        file.write(string.full_string() + "\n")

                    except Exception as e:
                        print(f"An error occurred: {str(e)}")
                        continue
        return f'{file_names[0]}_{get_cod_year_bdgd(typ="yearcod")}_{feeder}_{get_configuration()}.dss'
    
    else:
        path = os.path.join(output_directory, f'{file_name}_{get_cod_year_bdgd(typ="yearcod")}_{feeder}_{get_configuration()}.dss')

        with open(path, "w") as file:
            if "GD_" in file_name: #cria curvas padrões do EPRI nos PVsystems
                file.write(standard_curves_pv() + "\n")
            else:
                ...
            for string in object_list:
                try:
                    if type(string) == str:
                        file.write(string + "\n")
                    else:
                        file.write(string.full_string() + "\n")
                except Exception as e:
                    print(f"An error occurred: {str(e)}")
                    if type(string) == str:
                        file.write(f'{string} + "\n"!Elemento com erro de dados "\n"')
                    else:
                        file.write(f'{string.full_string()} + "\n"!Elemento com erro de dados "\n"')
                    continue
        print(f'O arquivo {file_name}_{get_cod_year_bdgd(typ="yearcod")}_{feeder}_{get_configuration()} foi gerado\n')
        

        return f'{file_name}_{get_cod_year_bdgd(typ="yearcod")}_{feeder}_{get_configuration()}.dss'


def create_master_file(file_name="", feeder="", master_content="", output_folder=""):
    """
    Create an dss_models_output file and write data from a list of objects.


    Creates an dss_models_output file in the 'dss_models_output' directory and writes OpenDSS commands from the list,
    separated by newline characters. If any error occurs, it will be displayed.

    """
    output_directory = create_output_folder(feeder=feeder,output_folder=output_folder)

    path = os.path.join(output_directory, f'{file_name}_{get_cod_year_bdgd(typ="yearcod")}_{feeder}_{get_configuration()}.dss')

    try:
        with open(path, "w") as file:
            file.write(master_content + "\n")
        print(f'O arquivo {file_name}_{get_cod_year_bdgd(typ="yearcod")}_{feeder}_{get_configuration()} foi gerado em ({path})\n')
    except Exception as e:
        print(f"An error occurred: {str(e)}")


def create_output_feeder_coords(df: pd.DataFrame, feeder="", filename="buscoords", output_folder=""):
    """Crie um arquivo de saída csv e grave dados de um DataFrame.
        Parâmetros:
        - df (DataFrame): Lista de objetos a serem gravados no arquivo.ex: objetos de linha ou transformador.
        - filename (str): nome do arquivo de saída. Ex: Transformers.txt ou lines.txt

        Cria um arquivo de saída no diretório 'saída' e grava com os comandos da lista,
        separado por caracteres newline. Se ocorrer algum erro, ele será exibido.

    """
    output_directory = create_output_folder(feeder=feeder,output_folder=output_folder)
    dir_path = os.path.join(output_directory, f'{filename}.csv')
    # dir_path = os.path.join(output_directory, f'{filename}_{feeder}.csv')

    try:
        df.to_csv(dir_path, index=False)
        print(f"Arquivo {filename}.csv criado")
        # print(f'O arquivo {file_name}_{feeder} foi gerado\n')
    except Exception as e:
        print(f"Erro ao criar .csv da df de coordenadas: {str(e)}")


def create_output_all_coords(object_list=[], file_name="", object_lists="", file_names="", feeder=""):
    """Create an dss_models_output file and write data from a list of objects.

    Parameters:
    - object_list (list): List of objects to be written to the file.Ex: line or transformer objects.
    - file_name (str): Name of the dss_models_output file. Ex: transformers.txt or lines.txt

    Creates an dss_models_output file in the 'dss_models_output' directory and writes OpenDSS commands from the list,
    separated by newline characters. If any error occurs, it will be displayed.

    """

    if not os.path.exists("dss_models_output"):
        os.mkdir("dss_models_output")

    if not os.path.exists(f'dss_models_output/{feeder}'):
        os.mkdir(f'dss_models_output/{feeder}')

    output_directory = os.path.join(os.getcwd(), f'dss_models_output/{feeder}')

    if object_lists != "":
        for object_list, file_name in zip(object_lists, file_names):
            path = os.path.join(output_directory, f'{file_name}_{feeder}.dss')

            try:
                with open(path, "w") as file:
                    for string in object_list:
                        file.write(string.full_string() + "\n")
                # print(f'O arquivo {file_name}_{feeder} foi gerado\n')
            except Exception as e:
                print(f"An error occurred: {str(e)}")

        return f'{file_names[0]}_{feeder}.dss'

    else:
        path = os.path.join(output_directory, f'{file_name}_{feeder}.dss')

        try:
            with open(path, "w") as file:
                for string in object_list:
                    if type(string) == str:
                        file.write(string + "\n")
                    else:
                        file.write(string.full_string() + "\n")

            print(f'O arquivo {file_name}_{feeder} foi gerado\n')
        except Exception as e:
            print(f"An error occurred: {str(e)}")

        return f'{file_name}_{feeder}.dss'


def create_dfs_coords(filename="", feeder=""):
    """Purpose: filename
    """
    print("criando coordenadas...")

    cols = [
        "COD_ID",
        "FAS_CON",
        "PAC_1",
        "PAC_2",
        "TIP_CND",
        "COMP",
        "CTMT"
    ]

    path_object = pathlib.Path(filename)

    gdf_SSDMT = gpd.read_file(path_object, layer='SSDMT',
                              columns=cols,
                              ignore_geometry=False, engine='pyogrio', use_arrow=True)
    gdf_SSDMT = gdf_SSDMT.loc[gdf_SSDMT['CTMT'] == feeder]

    gdf_SSDBT = gpd.read_file(path_object, layer='SSDBT',
                              columns=cols,
                              ignore_geometry=False, engine='pyogrio', use_arrow=True)
    gdf_SSDBT = gdf_SSDBT.loc[gdf_SSDBT['CTMT'] == feeder]

    return gdf_SSDMT, gdf_SSDBT

def create_voltage_bases(dicionario_kv): #remover as tensões de secundário de fase aqui
    lista=[]
    #print('aqui')
    # TODO evitar tomar decisoes 
    if len(dicionario_kv) > 0:
        for value in dicionario_kv.values(): 
            if value >= 0.22:
                lista.append(value)
            else:
                ...
        x=set(lista)
        return(list(x))
    else:
        return(lista)

def standard_curves_pv():
        return(f'New "LoadShape.PVIrrad_diaria" npts=24 interval=1 \n'
               f'~ mult = [0 0 0 0 0 0 0.1 0.2 0.3 0.5 0.8 0.9 1.0 1.0 0.99 0.9 0.7 0.4 0.1 0 0 0 0 0] \n'
               f'New XYCurve.MyPvsT npts=4  xarray=[0  25  75  100]  yarray=[1.2 1.0 0.8  0.6] \n'
               f'New XYCurve.MyEff npts=4  xarray=[0.1  0.2  0.4  1.0]  yarray=[0.86  0.9  0.93  0.97] \n'
               f'New Tshape.MyTemp npts=24 interval=1 \n'
               f'~ temp=[25, 25, 25, 25, 25, 25, 25, 25, 35, 40, 45, 50, 60, 60, 55, 40, 35, 30, 25, 25, 25, 25, 25, 25] \n'
               )

def check_duplicate_loads_names(df_load, consumer_type: str = ""):
    if consumer_type == 'BT':
        column = 'RAMAL'
    else:
        column = 'PN_CON'
    valores_repetidos = df_load[column][df_load[column].duplicated(keep=False)].index
    if len(valores_repetidos) >= 2:
        for valor in df_load[column][df_load[column].duplicated(keep=False)].unique():
            count = 1 #colocar for valores in valores_repetidos antes daqui
            index_repetido = df_load[column][df_load[column] == valor].index
            for index in index_repetido: #muda nome de cargas que tenha o mesmo ramal acrescentando um sufixo
                df_load.loc[index,column] = f'{df_load.loc[index,column]}_{count}'
                count += 1
    else:
        ...

def adapt_regulators_names(df_tr,type_trafo): #Nomeia dinamicamente os reguladores e transformadores que são formados em bancos
    if type_trafo == 'transformer':
        column = 'COD_ID'
    else:
        column = 'UN_RE'
    contagem_valores = df_tr[column].value_counts().to_dict()
    for value, quantidade in contagem_valores.items():
        i = df_tr[df_tr['COD_ID'] == value].index
        if quantidade > 6 or (df_tr.loc[i[0],'BANC'] == 1 and len(i) == 1): #remove transformadores que sejam formado por um banco com mais de 6 trafos ou que sejam formados por bancos e não sejam bancos
            df_tr.drop(df_tr[df_tr['COD_ID'] == value].index, inplace=True)
            continue
        count = 0
        count_index = 0
        indices = df_tr[df_tr[column] == value].index.tolist()
        while count < quantidade:
            df_tr.loc[indices[count_index],column] = f'{df_tr.loc[indices[count_index],column]}{chr(count+65)}' #adiciona sufixos de letras ao final do nome assim como o geoperdas
            count += 1
            count_index += 1
        else:
            continue

def get_cod_year_bdgd(cod: Optional[str] = None, data:Optional[str] = None, typ: Optional[str] = None): #captura o código e o ano da BDGD 
    global cod_year_bdgd
    if cod != None and data != None:
        cod_year_bdgd = [cod, data]
    elif typ == 'cod':
        return(cod_year_bdgd[0])
    elif typ == 'data':
        return(cod_year_bdgd[1])
    else:
        return(cod_year_bdgd[1]+cod_year_bdgd[0])
    
def limitar_tensao_superior(kvpu): #settings (Limitar tensão de barras e reguladores)
    if kvpu > 1.05:
        kvpu = 1.05
    else:
        kvpu
    return(kvpu)

def adequar_modelo_carga(int_model):#settings (Adequar modelo de carga)
    if int_model == 1:
        return(2,3)
    elif int_model == 2:
        return(1,1)
    else:
        return(3,3)

def create_df_trafos_vazios(df_ucbt: Optional[pd.DataFrame] = None,df_ip: Optional[pd.DataFrame] = None,df_tr: Optional[pd.DataFrame] = None):
    global tr_vazios
    if df_ucbt is not None or df_ip is not None:
        lista_tr = df_tr['COD_ID'].str[:-1]
        df_tr2 = df_tr[~lista_tr.isin(df_ucbt['UNI_TR_MT']) & ~lista_tr.isin(df_ip['UNI_TR_MT'])]
        trs = df_tr2['COD_ID'].str[:-1].tolist()
        df_tr_cargas = pd.DataFrame(df_ucbt).groupby('UNI_TR_MT', as_index=True).agg({'ENE_01':'sum','ENE_02':'sum','ENE_03':'sum','ENE_04':'sum','ENE_05':'sum',
            'ENE_06':'sum','ENE_07': 'sum','ENE_08': 'sum','ENE_09':'sum','ENE_10':'sum','ENE_11':'sum','ENE_12':'sum'})
        df_tr_ips = pd.DataFrame(df_ip).groupby('UNI_TR_MT', as_index=True).agg({'ENE_01':'sum','ENE_02':'sum','ENE_03':'sum','ENE_04':'sum','ENE_05':'sum',
            'ENE_06':'sum','ENE_07': 'sum','ENE_08': 'sum','ENE_09':'sum','ENE_10':'sum','ENE_11':'sum','ENE_12':'sum'})
        df_tr_cargas = df_tr_cargas.sum(axis=1)
        df_tr_ips = df_tr_ips.sum(axis=1)
        soma = df_tr_cargas.add(df_tr_ips, fill_value=0)
        tr_vazios_ucbt = list(soma[soma == 0].index)
        #tr_vazios_ucbt = list(df_tr_cargas[df_tr_cargas == 0].index)
        #tr_vazios_pip = list(df_tr_ips[df_tr_ips == 0].index)
        #tr_vazios = tr_vazios_ucbt + tr_vazios_pip + trs
        tr_vazios = tr_vazios_ucbt + trs
    else:
        return(tr_vazios)
    
def perdas_trafos_abnt(fases,kv,pot,perda):
    if fases == '3':
        if float(kv) < 15:
            try:
                file_tri_15kv = os.path.join(os.getcwd(), "PRODIST_MOD7_TR_TRIF_15kvmax.csv")
                df_tri_15kv = pd.read_csv(file_tri_15kv, index_col=0)
                return(df_tri_15kv.loc[pot,perda])
            except:
                if perda == 'noloadloss':
                    loss = (-0.0023*pot**2 + 2.8531*pot + 43.333)
                    return(loss)
                else:
                    loss = (-0.0095*pot**2 + 14.387*pot + 199.59)
                    return(loss)
        elif float(kv) < 24.2:
            try:
                file_tri_24kv = os.path.join(os.getcwd(), "PRODIST_MOD7_TR_TRIF_24.2kvmax.csv")
                df_tri_24kv = pd.read_csv(file_tri_24kv, index_col=0)
                return(df_tri_24kv.loc[pot,perda])
            except:
                if perda == 'noloadloss':
                    loss = (-0.0032*pot**2 + 3.3031*pot + 38.568)
                    return(loss)
                else:
                    loss = (-0.011*pot**2 + 15.375*pot + 208.34)
                    return(loss)
        else:
            try:
                file_tri_36kv = os.path.join(os.getcwd(), "PRODIST_MOD7_TR_TRIF_36.2kvmax.csv")
                df_tri_36kv = pd.read_csv(file_tri_36kv, index_col=0)
                return(df_tri_36kv.loc[pot,perda])
            except:
                if perda == 'noloadloss':
                    loss = (-0.0028*pot**2 + 3.3095*pot + 47.499)
                    return(loss)
                else:
                    loss = (-0.012*pot**2 + 16.277*pot + 223.18)
                    return(loss)
    else:
        if float(kv) < 15:
            try:
                file_mono_15kv = os.path.join(os.getcwd(), "PRODIST_MOD7_TR_MONO_15kvmax.csv")
                df_mono_15kv = pd.read_csv(file_mono_15kv, index_col=0)
                return(int(fases)*df_mono_15kv.loc[pot,perda])
            except:
                if perda == 'noloadloss':
                    loss = int(fases)*(-0.0092*pot**2 + 3.0491*pot + 15.18)
                    return(loss)
                else:
                    loss = int(fases)*(-0.0165*pot**2 + 13.848*pot + 83.37)
                    return(loss)
        elif float(kv) < 24.2:
            try:
                file_mono_24kv = os.path.join(os.getcwd(), "PRODIST_MOD7_TR_MONO_24.2kvmax.csv")
                df_mono_24kv = pd.read_csv(file_mono_24kv, index_col=0)
                return(int(fases)*df_mono_24kv.loc[pot,perda])
            except:
                if perda == 'noloadloss':
                    loss = int(fases)*(-0.0113*pot**2 + 3.4321*pot + 17.717)
                    return(loss)
                else:
                    loss = int(fases)*(-0.0503*pot**2 + 17.879*pot + 63.804)
                    return(loss)
        else:
            try:
                file_mono_36kv = os.path.join(os.getcwd(), "PRODIST_MOD7_TR_MONO_36.2kvmax.csv")
                df_mono_36kv = pd.read_csv(file_mono_36kv, index_col=0)
                return(int(fases)*df_mono_36kv.loc[pot,perda])
            except:
                if perda == 'noloadloss':
                    loss = int(fases)*(-0.0132*pot**2 + 3.6825*pot + 19.662)
                    return(loss)
                else:
                    loss = int(fases)*(-0.054*pot**2 + 18.383*pot + 70.191)
                    return(loss)
                
def get_configuration(feeder:Optional[str]=None,output_folder:Optional[str]=None):
    global sufixo_config
    df_config = pd.DataFrame(columns=["Configuração", "Descrição"])
    count = 0
    if settings.intRealizaCnvrgcPNT:
        sufixo_config = "N"
        df_config.loc[count] = ['intRealizaCnvrgcPNT','Convergência de Perda Não Técnica']
        count += 1
    else:
        sufixo_config = "-"
    if settings.intUsaTrafoABNT:
        sufixo_config = sufixo_config + "T"
        df_config.loc[count] = ['intUsaTrafoABNT','Perdas nos Transformadores de acordo ABNT']
        count += 1
    else:
        sufixo_config = sufixo_config + "-"
    if settings.intAdequarTensaoCargasMT:
        sufixo_config = sufixo_config + "M"
        settings.intAdequarModeloCarga
        df_config.loc[count] = ['intAdequarTensaoCargasMT','Adequação de Tensão Mínima das Cargas MT (0.93 pu)']
        count += 1
    else:
        sufixo_config = sufixo_config + "-"
    if settings.intAdequarTensaoCargasBT:
        sufixo_config = sufixo_config + "B"
        df_config.loc[count] = ['intAdequarTensaoCargasBT','Adequação de Tensão Mínima das Cargas BT (0.92 pu)']
        count += 1
    else:
        sufixo_config = sufixo_config + "-"
    if settings.intAdequarTensaoSuperior:
        sufixo_config = sufixo_config + "S"
        df_config.loc[count] = ['intAdequarTensaoSuperior','Limitar Máxima Tensão de Barras e Reguladores (1.05 pu)']
        count += 1
    else:
        sufixo_config = sufixo_config + "-"
    if settings.intAdequarRamal:
        sufixo_config = sufixo_config + "R"
        df_config.loc[count] = ['intAdequarRamal','Limitar o Ramal (30m)']
        count += 1
    else:
        sufixo_config = sufixo_config + "-"
    if settings.intAdequarModeloCarga == 1:
        sufixo_config = sufixo_config + "1"
        df_config.loc[count] = ['intAdequarModeloCarga = 1','Adequa modelo das cargas(model=2/model=3)']
        count += 1
    elif settings.intAdequarModeloCarga == 2:
        sufixo_config = sufixo_config + "2"
        df_config.loc[count] = ['intAdequarModeloCarga = 2','Adequa modelo das cargas(model=1/model=1)']
        count += 1
    else:
        sufixo_config = sufixo_config + "3"
        df_config.loc[count] = ['intAdequarModeloCarga = 3','Adequa modelo das cargas(model=3/model=3)']
        count += 1
    if settings.intAdequarPotenciaCarga:
        sufixo_config = sufixo_config + "P"
        df_config.loc[count] = ['intAdequarPotenciaCarga', "Limitar Cargas BT (Potência ativa do transformador)"]
        count += 1
    else:
        sufixo_config = sufixo_config + "-"
    if settings.intAdequarTrafoVazio:
        sufixo_config = sufixo_config + "V"
        df_config.loc[count] = ['intAdequarTrafoVazio', "Eliminar Transformadores Vazios"]
        count += 1
    else:
        sufixo_config = sufixo_config + "-"
    if settings.intAdequarTapTrafo:
        sufixo_config = sufixo_config + "T"
        df_config.loc[count] = ['intAdequarTapTrafo', "Utilizar Tap nos Transformadores"]
        count += 1
    else:
        sufixo_config = sufixo_config + "-"
    if settings.intNeutralizarTrafoTerceiros:
        sufixo_config = sufixo_config + "T"
        df_config.loc[count] = ['intNeutralizarTrafoTerceiros', "Neutralizar Transformadores de Terceiros"]
        count += 1
    else:
        sufixo_config = sufixo_config + "-"
    if settings.intNeutralizarRedeTerceiros:
        sufixo_config = sufixo_config + "R"
        df_config.loc[count] = ['intNeutralizarRedeTerceiros', "Neutralizar Redes de Terceiros (MT/BT)"]
        count += 1
    else:
        sufixo_config = sufixo_config + "-"
    df_config.loc[count] = ['dblVPUMin', f"Tensão mínima(pu)={settings.dblVPUMin}"]
    if feeder is not None:
        output_directory = create_output_folder(feeder,output_folder)
        dir_path = os.path.join(output_directory, r'configurações.csv')
        df_config.to_csv(dir_path,index=False)
    else:
        return(sufixo_config)

def create_output_folder(feeder:Optional[str] = None, output_folder:Optional[str] = None):
    global substation
    if output_folder is not None:
        try:
            if not os.path.exists(f'{output_folder}/sub__{substation}/{feeder}'):
                os.makedirs(f'{output_folder}/sub__{substation}/{feeder}', exist_ok=True)
                output_directory = f'{output_folder}/sub__{substation}/{feeder}'
            else:
                output_directory = f'{output_folder}/sub__{substation}'+ f'/{feeder}'
        except FileNotFoundError:
            if not os.path.exists("dss_models_output"):
                os.mkdir("dss_models_output")

            if not os.path.exists(f'dss_models_output/{feeder}'):
                os.mkdir(f'dss_models_output/{feeder}')
                print(f'Caminho para criação de pasta inválido. O arquivo DSS será criado em: dss_models_output/{feeder}')
            output_directory = os.path.join(os.getcwd(), f'dss_models_output/{feeder}')
    else:
        if not os.path.exists("dss_models_output"):
            os.mkdir("dss_models_output")
        if feeder is not None:
            if not os.path.exists(f'dss_models_output/sub__{substation}/{feeder}'):
                os.makedirs(f'dss_models_output/sub__{substation}/{feeder}', exist_ok=True)
            output_directory = os.path.join(os.getcwd(), f'dss_models_output/sub__{substation}/{feeder}')
        else:
            output_directory = os.path.join(os.getcwd(), f'dss_models_output')
    
    return(output_directory)

def create_aux_tramo(dataframe: gpd.geodataframe.GeoDataFrame, feeder): #tabela auxiliar para definir a ordem 
    alimentador = feeder
    df_trafo = merge_df_aux_tr(dataframe['EQTRMT']['gdf'], dataframe['UNTRMT']['gdf'].query("CTMT==@alimentador"),
                            left_column='UNI_TR_MT', right_column='COD_ID')
    adapt_regulators_names(df_trafo,'transformer')
    df_aux_ssdmt = dataframe['SSDMT']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
    df_aux_ssdmt['ELEM'] = 'SEGMMT'
    df_aux_ssdbt = dataframe['SSDBT']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
    df_aux_ssdbt['ELEM'] = 'SEGMBT'
    df_aux_ramalig = dataframe['RAMLIG']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
    df_aux_ramalig['ELEM'] = 'RML'
    df_aux_unsemt = dataframe['UNSEMT']['gdf'].query("CTMT == @alimentador & P_N_OPE == 'F'")[['COD_ID','CTMT','PAC_1','PAC_2']]
    df_aux_unsemt['ELEM'] = 'CHVMT'
    df_aux_unsebt = dataframe['UNSEBT']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
    df_aux_unsebt['ELEM'] = 'CHVBT'
    df_aux_trafo = df_trafo[['COD_ID','CTMT','PAC_1','PAC_2']]
    df_aux_trafo['ELEM'] = 'TRAFO'
    df_aux_regul = dataframe['UNREMT']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
    df_aux_regul['ELEM'] = 'REGUL'
    df_aux_tramo = pd.concat([df_aux_ssdmt,df_aux_ssdbt,df_aux_ramalig,df_aux_unsemt,df_aux_unsebt,df_aux_trafo,df_aux_regul], ignore_index=True)
    
    return(df_aux_tramo,df_aux_trafo)

def merge_df_aux_tr(dataframe_1,dataframe_2,right_column,left_column):

    merged_dfs = pd.merge(dataframe_1, dataframe_2, left_on=left_column, right_on=right_column, how='inner')

    for column in merged_dfs.columns:
        if column.endswith('_x'):
            merged_dfs.drop(columns=column, inplace=True)
        elif column.endswith('_y'):
            new_column_name = column[:-2]  # Remove the '_df2' suffix
            merged_dfs.rename(columns={column: new_column_name}, inplace=True)
    return(merged_dfs)

#TODO verifica se a atribuição dos PACs é invertida
def ordem_pacs(df_aux_tramo:Optional[pd.DataFrame] = None, pac_ctmt: Optional[str] = None):
    global seq 
    if df_aux_tramo is not None:
        if pac_ctmt in df_aux_tramo['PAC_1'].values:
            seq = 'Direta'
        else:
            seq = 'Invertida'
            return(print('PACs invertidos!!'))
    else:
        return(seq)

#TODO cria uma lista dos elementos isolados no circuito
def elem_isolados(dataframe: Optional[gpd.geodataframe.GeoDataFrame] = None, feeder: Optional[str] = None,pac_ctmt: Optional[str] = None, output_folder: Optional[str] = None): #cria uma lista de elementos isolados
    global lista_isolados
    if settings.TipoBDGD: #BDGD privada
        ucbt = "UCBT"
        ucmt = "UCMT"
        ugbt = "UGBT"
        ugmt = "UGMT"
    else: #BDGD pública
        ucbt = "UCBT_tab"
        ucmt = "UCMT_tab"
        ugbt = "UGBT_tab"
        ugmt = "UGMT_tab"
    if dataframe != None:

        alimentador = feeder
        df_trafo = merge_df_aux_tr(dataframe['EQTRMT']['gdf'], dataframe['UNTRMT']['gdf'].query("CTMT==@alimentador"),
                                left_column='UNI_TR_MT', right_column='COD_ID')
        df_reg = merge_df_aux_tr(dataframe['EQRE']['gdf'], dataframe['UNREMT']['gdf'].query("CTMT==@alimentador"),
                                left_column='UN_RE', right_column='COD_ID')

        adapt_regulators_names(df_trafo,'transformer')
        adapt_regulators_names(df_reg,'regulator')

        df_aux_ssdmt = dataframe['SSDMT']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
        df_aux_ssdmt['ELEM'] = 'SEGMMT'
        df_aux_ssdbt = dataframe['SSDBT']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
        df_aux_ssdbt['ELEM'] = 'SEGMBT'
        df_aux_ramalig = dataframe['RAMLIG']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
        df_aux_ramalig['ELEM'] = 'RML'
        #df_aux_unsemt = dataframe['UNSEMT']['gdf'].query("CTMT == @alimentador & P_N_OPE == 'F'")[['COD_ID','CTMT','PAC_1','PAC_2']]
        df_aux_unsemt = dataframe['UNSEMT']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
        df_aux_unsemt['ELEM'] = 'CHVMT'
        df_aux_unsebt = dataframe['UNSEBT']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
        df_aux_unsebt['ELEM'] = 'CHVBT'
        df_aux_trafo = df_trafo[['COD_ID','CTMT','PAC_1','PAC_2']]
        df_aux_trafo['ELEM'] = 'TRAFO'
        df_aux_regul = df_reg[['COD_ID','CTMT','PAC_1','PAC_2']]
        df_aux_regul['ELEM'] = 'REGUL'
        if settings.TipoBDGD:
            df_aux_ucmt = dataframe[ucmt]['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC']]
            df_aux_ucmt['PAC_2'] = ''
            df_aux_ucmt['ELEM'] = 'LDMT'
            df_aux_ucmt = df_aux_ucmt.rename(columns={'PAC':'PAC_1'})
            df_aux_ucbt = dataframe[ucbt]['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC']]
            df_aux_ucbt['PAC_2'] = ''
            df_aux_ucbt['ELEM'] = 'LDBT'
            df_aux_ucbt = df_aux_ucbt.rename(columns={'PAC':'PAC_1'})
        else:
            df_aux_ucmt = dataframe[ucmt]['gdf'].query("CTMT == @alimentador")[['PN_CON','CTMT','PAC']]
            df_aux_ucmt['PAC_2'] = ''
            df_aux_ucmt['ELEM'] = 'LDMT'
            df_aux_ucmt = df_aux_ucmt.rename(columns={'PAC':'PAC_1','PN_CON':'COD_ID'})
            df_aux_ucbt = dataframe[ucbt]['gdf'].query("CTMT == @alimentador")[['RAMAL','CTMT','PAC']]
            df_aux_ucbt['PAC_2'] = ''
            df_aux_ucbt['ELEM'] = 'LDBT'
            df_aux_ucbt = df_aux_ucbt.rename(columns={'PAC':'PAC_1','RAMAL':'COD_ID'})
        df_aux_pip = dataframe['PIP']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC']]
        df_aux_pip['PAC_2'] = ''
        df_aux_pip['ELEM'] = 'PIP'
        df_aux_pip = df_aux_pip.rename(columns={'PAC':'PAC_1'})
        df_total = pd.concat([df_aux_ssdmt,df_aux_ssdbt,df_aux_ramalig,df_aux_unsemt,df_aux_unsebt,df_aux_trafo,df_aux_regul,df_aux_pip,df_aux_ucbt,df_aux_ucmt], ignore_index=True)
        ##################################### TODO REMOVER ISSO APOS TESTE
        # trafo = '005038'
        # df_trafo_teste = dataframe['UNTRMT']['gdf'].query("COD_ID == @trafo")[['COD_ID','CTMT','PAC_1','PAC_2']]
        # df_trafo_teste['ELEM'] = 'TRAFO'
        # df_ssdbt_teste = dataframe['SSDBT']['gdf'].query("UNI_TR_MT == @trafo")[['COD_ID','CTMT','PAC_1','PAC_2']]
        # df_ssdbt_teste['ELEM'] = 'SEGMBT'
        # df_ramalig_teste = dataframe['RAMLIG']['gdf'].query("UNI_TR_MT == @trafo")[['COD_ID','CTMT','PAC_1','PAC_2']]
        # df_ramalig_teste['ELEM'] = 'RML'
        # df_ucbt_teste = dataframe[ucbt]['gdf'].query("UNI_TR_MT == @trafo")[['COD_ID','CTMT','PAC']]
        # df_ucbt_teste['PAC_2'] = ''
        # df_ucbt_teste['ELEM'] = 'LDBT'
        # df_ucbt_teste = df_ucbt_teste.rename(columns={'PAC':'PAC_1'})
        # df_pip_teste = dataframe['PIP']['gdf'].query("UNI_TR_MT == @trafo")[['COD_ID','CTMT','PAC']]
        # df_pip_teste['PAC_2'] = ''
        # df_pip_teste['ELEM'] = 'PIP'
        # df_pip_teste = df_pip_teste.rename(columns={'PAC':'PAC_1'})
        # df_total = pd.concat([df_trafo_teste,df_ssdbt_teste,df_ramalig_teste,df_ucbt_teste,df_pip_teste])
        ###################################################
        grafo = nx.Graph()
        for index,row in df_total.iterrows():
            grafo.add_node(row['PAC_1'])
            grafo.add_node(row['PAC_2'])
            grafo.add_edge(row['PAC_1'], row['PAC_2'])
        try:
            grafo.remove_node('')
        except:
            pass
        conectados = list(nx.connected_components(grafo))
        if any(pac_ctmt in grf for grf in conectados):
            for conection in conectados:
                if pac_ctmt in conection:
                    df_not_connected = df_total[~df_total['PAC_1'].isin(conection) & ~df_total['PAC_2'].isin(conection)]
                    break
                else:
                    #df_x = df_total[~df_total['PAC_1'].isin(conection) & ~df_total['PAC_2'].isin(conection)]
                    continue
            if df_not_connected.empty:
                return(print('Não existem elementos isolados!'))
            else:
                log_erros(df_not_connected,alimentador,output_folder)
                lista_isolados = []
                df_not_connected.fillna('Nulo',inplace=True)
                for cod_id in df_not_connected['COD_ID'].values:
                    if df_not_connected.loc[df_not_connected['COD_ID'] == cod_id, 'ELEM'].iloc[0] == 'SEGMBT': 
                        lista_isolados.append(f'SBT_{cod_id}')
                    elif df_not_connected.loc[df_not_connected['COD_ID'] == cod_id, 'ELEM'].iloc[0] == 'RAMLIG':
                        lista_isolados.append(f'RBT_{cod_id}')
                    elif df_not_connected.loc[df_not_connected['COD_ID'] == cod_id, 'ELEM'].iloc[0] == 'SEGMMT':
                        lista_isolados.append(f'SMT_{cod_id}')
                    elif df_not_connected.loc[df_not_connected['COD_ID'] == cod_id, 'ELEM'].iloc[0] == 'CHVMT':
                        lista_isolados.append(f'CMT_{cod_id}')
                    elif df_not_connected.loc[df_not_connected['COD_ID'] == cod_id, 'ELEM'].iloc[0] == 'CHVBT':
                        lista_isolados.append(f'CBT_{cod_id}')
                    elif df_not_connected.loc[df_not_connected['COD_ID'] == cod_id, 'ELEM'].iloc[0] == 'LDBT':
                        lista_isolados.append(f'BT_{cod_id}')
                    elif df_not_connected.loc[df_not_connected['COD_ID'] == cod_id, 'ELEM'].iloc[0] == 'LDMT':
                        lista_isolados.append(f'MT_{cod_id}')
                    elif df_not_connected.loc[df_not_connected['COD_ID'] == cod_id, 'ELEM'].iloc[0] == 'PIP':
                        lista_isolados.append(f'BT_IP{cod_id}')
                    elif df_not_connected.loc[df_not_connected['COD_ID'] == cod_id, 'ELEM'].iloc[0] == 'REGUL':
                        lista_isolados.append(f'REG_{cod_id}')
                    else:
                        lista_isolados.append(cod_id)
            return(print('Lista de elementos isolados criados!'))
        else:
            log_erros(feeder=alimentador,output_directory=output_folder,ctmt=pac_ctmt)
            return(print('Alimentador não tem conexão com a fonte!!'))
    else:
        return(lista_isolados)
#TODO realiza a sequência elétrica do circuito
def seq_eletrica(dataframe: Optional[gpd.geodataframe.GeoDataFrame] = None, feeder: Optional[str] = None,pac: Optional[str] = None, kvbase: Optional[float] = None, key:Optional[str] = None): #define as tensões de PRIMÁRIO dos elementos de MT
    global tensao_dict
    if settings.TipoBDGD: #BDGD privada
        ucmt = "UCMT"
        ugmt = "UGMT"
    else: #BDGD pública
        ucmt = "UCMT_tab"
        ugmt = "UGMT_tab"
    if pac == None:
        try:
            return(tensao_dict[key])
        except KeyError:
            print(f'Não foi realizada a sequência elétrica para o nó {key}')
            return float("nan")
    else:
        alimentador = feeder
        df_trafo = merge_df_aux_tr(dataframe['EQTRMT']['gdf'], dataframe['UNTRMT']['gdf'].query("CTMT==@alimentador"),
                                left_column='UNI_TR_MT', right_column='COD_ID')
        adapt_regulators_names(df_trafo,'transformer')
        df_transformer = df_trafo[['COD_ID','CTMT','PAC_1','PAC_2','TEN_PRI','TEN_LIN_SE']]
        df_aux_ssdmt = dataframe['SSDMT']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
        df_aux_ssdmt['ELEM'] = 'SEGMMT'
        df_aux_unsemt = dataframe['UNSEMT']['gdf'].query("CTMT == @alimentador & P_N_OPE == 'F'")[['COD_ID','CTMT','PAC_1','PAC_2']]
        df_aux_unsemt['ELEM'] = 'CHVMT'
        df_aux_regul = dataframe['UNREMT']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
        df_aux_regul['ELEM'] = 'REGUL'
        if settings.TipoBDGD:
            df_aux_ucmt = dataframe[ucmt]['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC']]
            df_aux_ucmt = df_aux_ucmt.rename(columns={'PAC':'PAC_1'})
        else:
            df_aux_ucmt = dataframe[ucmt]['gdf'].query("CTMT == @alimentador")[['PN_CON','CTMT','PAC']]
            df_aux_ucmt = df_aux_ucmt.rename(columns={'PAC':'PAC_1','PN_CON':'COD_ID'})
        df_aux_ucmt['PAC_2'] = ''
        df_aux_ucmt['ELEM'] = 'LDMT'
        df_elements = pd.concat([df_aux_ssdmt,df_aux_unsemt,df_transformer,df_aux_regul,df_aux_ucmt], ignore_index=True)
        pac_ctmt = pac
        grafo = nx.Graph()

        for index, row in df_elements.iterrows():
            grafo.add_node(row['PAC_1'])
            grafo.add_node(row['PAC_2'])
            grafo.add_edge(row['PAC_1'], row['PAC_2'])
        try:
            grafo.remove_node('')
        except:
            pass
        conectados = list(nx.connected_components(grafo))
        tensao_dict = {}  # Dicionário para armazenar as tensões
        tensao_dict[pac_ctmt] = kvbase
        if any(pac_ctmt in grf for grf in conectados):
            sequencia = list(nx.bfs_edges(grafo,pac_ctmt)) #usar essa função!!!
        else:
            return(print("Não é possível gerar a sequência elétrica, pois o alimentador não tem conexão com a fonte"))
        kv = kvbase
        count = 0
        for seq in sequencia:
            if seq[0] in tensao_dict.keys():
                if seq[1] in df_transformer['PAC_2'].values:
                    if df_transformer.loc[df_transformer['PAC_2'] == seq[1], 'TEN_LIN_SE'].iloc[0] > 1:
                        kv = df_transformer.loc[df_transformer['PAC_2'] == seq[1], 'TEN_LIN_SE'].iloc[0]
                        tensao_dict[seq[1]] = kv
                    else:
                        tensao_dict[seq[1]] = df_transformer.loc[df_transformer['PAC_2'] == seq[1], 'TEN_LIN_SE'].iloc[0]
                else:
                    tensao_dict[seq[1]] = kv
            else:
                kv = tensao_dict[sequencia[count-1][1]] #deve buscar a tensão do nó anterior... 
                tensao_dict[seq[0]] = kv
                if seq[1] in df_transformer['PAC_2'].values:
                    kv = df_transformer.loc[df_transformer['PAC_2'] == seq[1], 'TEN_LIN_SE'].iloc[0]
                    tensao_dict[seq[1]] = kv
                else:
                    tensao_dict[seq[1]] = kv
        return(print('Sequência elétrica na média tensão realizada!'))
    
def get_substation(sub:Optional[str] = None):
    global substation
    if sub == None:
        substation = ""
    else:
        substation = sub

def list_subs(df,output_path): 
    if output_path == None:
        output_path = create_output_folder(output_folder=output_path)
    file_path = os.path.join(output_path, f"lista_subestações_{get_cod_year_bdgd(typ='cod')}.csv")
    if os.path.exists(file_path):
        return
    df_sub = df[['COD_ID','SUB']]
    df_sub.to_csv(file_path, index=False, encoding='utf-8')

# def pvsystem_stats(dfs,output_folder):
#     colunas = ['CTMT','POT_PV_TOTAL_INSTALADA','POT_OUTRAS_TOTAL_INSTALADA']
#     df = pd.DataFrame(columns=colunas)
#     for index,feeder in enumerate(dfs["CTMT"]['gdf']['COD_ID'].tolist()):
#         alimentador = feeder
#         df_ugbt = dfs['UGBT_tab']['gdf'].query("CTMT==@alimentador & SIT_ATIV == 'AT'")
#         df_ugmt = dfs['UGMT_tab']['gdf'].query("CTMT==@alimentador & SIT_ATIV == 'AT'")
#         df_pvbt = df_ugbt[df_ugbt['CEG_GD'].str.contains('GD.CE.001',case=False,na=False)]
#         df_pvmt = df_ugmt[df_ugmt['CEG_GD'].str.contains('GD.CE.001',case=False,na=False)]
#         df.loc[index,'CTMT'] = feeder
#         df.loc[index,'POT_PV_TOTAL_INSTALADA'] = float(df_pvmt["POT_INST"].sum() + df_pvbt["POT_INST"].sum())
#         df.loc[index,'POT_OUTRAS_TOTAL_INSTALADA'] = float(df_ugmt["POT_INST"].sum() + df_ugbt["POT_INST"].sum()) - df.loc[index,'POT_PV_TOTAL_INSTALADA']
#     file_path = os.path.join(output_folder, f'pvsystem_{get_cod_year_bdgd(typ='cod')}.csv')
#     df.to_csv(file_path, index=False, encoding='utf-8')
