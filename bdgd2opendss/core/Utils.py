# -*- encoding: utf-8 -*-

import json
import os.path
import pathlib

import geopandas as gpd
import pandas as pd


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
    if left_column == 'UN_RE':
        merged_dfs = pd.merge(entity1_df, enetity2_df, left_on=left_column, right_on=right_column, how='inner')
    else:
        merged_dfs = pd.merge(entity1_df, enetity2_df, left_on=left_column, right_on=right_column, how='right') #Pegar UNI_TR_MT que não tenham EQTRMT (geram linhas e cargas isoladas)
        merged_dfs['POT_NOM_x'] = merged_dfs["POT_NOM_x"].fillna(0).astype(int) 
    for column in merged_dfs.columns:
        if column.endswith('_x'):
            merged_dfs.drop(columns=column, inplace=True)
        elif column.endswith('_y'):
            new_column_name = column[:-2]  # Remove the '_df2' suffix
            merged_dfs.rename(columns={column: new_column_name}, inplace=True)

    return merged_dfs


def create_output_file(object_list=[], file_name="", object_lists="", file_names="", output_folder="", feeder=""): #TODO dar a opção de criar o alimentador em qualquer pasta do computador
    """Create an dss_models_output file and write data from a list of objects.

    Parameters:
    - object_list (list): List of objects to be written to the file.Ex: line or transformer objects.
    - file_name (str): Name of the dss_models_output file. Ex: transformers.txt or lines.txt

    Creates an dss_models_output file in the 'dss_models_output' directory and writes OpenDSS commands from the list,
    separated by newline characters. If any error occurs, it will be displayed.

    """
    if output_folder is not None:
        try:
            if not os.path.exists(f'{output_folder}\{feeder}'):
                os.mkdir(f'{output_folder}\{feeder}')
                output_directory = f'{output_folder}\{feeder}'
            else:
                output_directory = f'{output_folder}\{feeder}'
        except FileNotFoundError:
            if not os.path.exists("dss_models_output"):
                os.mkdir("dss_models_output")

            if not os.path.exists(f'dss_models_output/{feeder}'):
                os.mkdir(f'dss_models_output/{feeder}')
                print(f'Caminho para criação de pasta inválido. O arquivo DSS será criado em: dss_models_output/{feeder}')
            output_directory = os.path.join(os.getcwd(), f'dss_models_output\{feeder}')
    else:
        if not os.path.exists("dss_models_output"):
            os.mkdir("dss_models_output")

        if not os.path.exists(f'dss_models_output/{feeder}'):
            os.mkdir(f'dss_models_output/{feeder}')

        output_directory = os.path.join(os.getcwd(), f'dss_models_output\{feeder}')

    if object_lists != "":
        if file_name == 'Cargas_BT_IP':
            k = 'a' #anexação de arquivo
            file_name = ""
        else:
            k = 'w' #sobre-escrevendo o arquivo
            file_name = ""
        for object_list, file_name in zip(object_lists, file_names):
            path = os.path.join(output_directory, f'{file_name}_{feeder}.dss')

            try:
                with open(path, k) as file:
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
                if "GD_" in file_name: #cria curvas padrões do EPRI
                    file.write(standard_curves_pv() + "\n")
                else:
                    ...
                for string in object_list:
                    if type(string) == str:
                        file.write(string + "\n")
                    else:
                        file.write(string.full_string() + "\n")

            print(f'O arquivo {file_name}_{feeder} foi gerado\n')
        except Exception as e:
            print(f"An error occurred: {str(e)}")

        return f'{file_name}_{feeder}.dss'


def create_master_file(file_name="", feeder="", master_content="", output_folder=""):
    """
    Create an dss_models_output file and write data from a list of objects.


    Creates an dss_models_output file in the 'dss_models_output' directory and writes OpenDSS commands from the list,
    separated by newline characters. If any error occurs, it will be displayed.

    """
    if output_folder is not None:
        try:
            if not os.path.exists(f'{output_folder}\{feeder}'):
                os.mkdir(f'{output_folder}\{feeder}')
                output_directory = f'{output_folder}\{feeder}'
            else:
                output_directory = f'{output_folder}\{feeder}'
        except FileNotFoundError:
            if not os.path.exists("dss_models_output"):
                os.mkdir("dss_models_output")

            if not os.path.exists(f'dss_models_output/{feeder}'):
                os.mkdir(f'dss_models_output/{feeder}')
                print(f'Caminho para criação de pasta inválido. O arquivo DSS será criado em: dss_models_output/{feeder}')
            output_directory = os.path.join(os.getcwd(), f'dss_models_output\{feeder}')
    else:
        if not os.path.exists("dss_models_output"):
            os.mkdir("dss_models_output")

        if not os.path.exists(f'dss_models_output/{feeder}'):
            os.mkdir(f'dss_models_output/{feeder}')

        output_directory = os.path.join(os.getcwd(), f'dss_models_output\{feeder}')

    path = os.path.join(output_directory, f'{file_name}_{feeder}.dss')

    try:
        with open(path, "w") as file:
            file.write(master_content + "\n")
        print(f'O arquivo {file_name}_{feeder} foi gerado em ({path})\n')
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
    if output_folder is not None:
        try:
            if not os.path.exists(f'{output_folder}\{feeder}'):
                os.mkdir(f'{output_folder}\{feeder}')
                output_directory = f'{output_folder}\{feeder}'
            else:
                output_directory = f'{output_folder}\{feeder}'
        except FileNotFoundError:
            if not os.path.exists("dss_models_output"):
                os.mkdir("dss_models_output")

            if not os.path.exists(f'dss_models_output/{feeder}'):
                os.mkdir(f'dss_models_output/{feeder}')
                print(f'Caminho para criação de pasta inválido. O arquivo DSS será criado em: dss_models_output/{feeder}')
            output_directory = os.path.join(os.getcwd(), f'dss_models_output\{feeder}')
    else:
        if not os.path.exists("dss_models_output"):
            os.mkdir("dss_models_output")

        if not os.path.exists(f'dss_models_output/{feeder}'):
            os.mkdir(f'dss_models_output/{feeder}')
        output_directory = os.path.join(os.getcwd(), f'dss_models_output\{feeder}')
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

    output_directory = os.path.join(os.getcwd(), f'dss_models_output\{feeder}')

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

    # TODO evitar tomar decisoes
    for value in dicionario_kv.values(): #dicionario_kv.values() usar
        if value >= 0.22:
            lista.append(value)
        else:
            ...
    x=set(lista)
    if max(lista) == '0.38':
        try:
            x.remove('0.22')
        except KeyError:
            ...
    return(list(x))

def standard_curves_pv():
        return(f'New "LoadShape.PVIrrad_diaria" npts=24 interval=1 \n'
               f'~ mult = [0 0 0 0 0 0 0.1 0.2 0.3 0.5 0.8 0.9 1.0 1.0 0.99 0.9 0.7 0.4 0.1 0 0 0 0 0] \n'
               f'New XYCurve.MyPvsT npts=4  xarray=[0  25  75  100]  yarray=[1.2 1.0 0.8  0.6] \n'
               f'New XYCurve.MyEff npts=4  xarray=[0.1  0.2  0.4  1.0]  yarray=[0.86  0.9  0.93  0.97] \n'
               f'New Tshape.MyTemp npts=24 interval=1 \n'
               f'~ temp=[25, 25, 25, 25, 25, 25, 25, 25, 35, 40, 45, 50, 60, 60, 55, 40, 35, 30, 25, 25, 25, 25, 25, 25] \n'
               )


