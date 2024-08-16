# -*- encoding: utf-8 -*-

import inspect
import json
import os.path
import pathlib
import time
from typing import Optional
import pandas as pd
import geopandas as gpd
import pyogrio
import pyarrow


from bdgd2opendss import Sample, Case, Circuit, LineCode, Line, LoadShape, Transformer, RegControl, Load, PVsystem
from bdgd2opendss.core.Utils import load_json, merge_entities_tables, inner_entities_tables, create_output_file, create_output_feeder_coords, create_dfs_coords
import bdgd2opendss.model.BusCoords as Coords

class Table:
    def __init__(self, name, columns, data_types, ignore_geometry_):
        self.name = name
        self.columns = columns
        self.data_types = data_types
        self.ignore_geometry = ignore_geometry_

    def __str__(self):
        return f"Table(name={self.name}, columns={self.columns}, data_types={self.data_types}, " \
               f"ignore_geometry={self.ignore_geometry})"


class JsonData:
    def __init__(self, file_name):
        """
        Inicializa a classe JsonData com o nome do arquivo de entrada.

        :param file_name: Nome do arquivo JSON de entrada.
        """
        self.data = self._read_json_file(file_name)
        self.tables = self._create_tables()

    @staticmethod
    def _read_json_file(file_name):
        """
        Lê o arquivo JSON fornecido e retorna o conteúdo como um objeto Python.

        :param file_name: Nome do arquivo JSON de entrada.
        :return: Objeto Python contendo o conteúdo do arquivo JSON.
        """
        with open(file_name, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data

    def _create_tables(self):
        """
        Cria um dicionário de tabelas a partir dos dados carregados do arquivo JSON.

        :return: Dicionário contendo informações das tabelas a serem processadas.
        """
        return {
            table_name: Table(
                table_name,
                table_data["columns"],
                table_data["type"],
                table_data["ignore_geometry"],
            )
            for table_name, table_data in self.data["configuration"][
                "tables"
            ].items()
        }

    def get_tables(self):
        """
        Retorna o dicionário de tabelas.

        :return: Dicionário contendo informações das tabelas a serem processadas.
        """
        return self.tables

    @staticmethod
    def convert_data_types(df, column_types):
        """
        Converte os tipos de dados das colunas do DataFrame fornecido.

        :param df: DataFrame a ser processado.
        :param column_types: Dicionário contendo mapeamento de colunas para tipos de dados.
        :return: DataFrame com tipos de dados convertidos.
        """
        return df.astype(column_types)

    def create_geodataframes(self, file_name, runs=1):
        """
        Cria GeoDataFrames a partir de um arquivo de entrada e coleta estatísticas.

        :param file_name: Nome do arquivo de entrada.
        :param runs: Número de vezes que cada tabela será carregada e convertida (padrão: 1).
        :return: Dicionário contendo GeoDataFrames e estatísticas.
        """
        geodataframes = {}

        for table_name, table in self.tables.items():

            load_times = []
            conversion_times = []
            gdf_converted = None

            for _ in range(runs):
                start_time = time.time()
                gdf_ = gpd.read_file(file_name, layer=table.name,
                                     include_fields=table.columns, columns=table.columns,
                                     ignore_geometry=table.ignore_geometry,engine='pyogrio',use_arrow=True)  #! ignore_geometry não funciona, pq este parâmetro espera um bool e está recebendo str
                start_conversion_time = time.time()
                gdf_converted = self.convert_data_types(gdf_, table.data_types)
                end_time = time.time()

                load_times.append(start_conversion_time - start_time)
                conversion_times.append(end_time - start_conversion_time)

            load_time_avg = sum(load_times) / len(load_times)
            conversion_time_avg = sum(conversion_times) / len(conversion_times)
            mem_usage = gdf_converted.memory_usage(index=True, deep=True).sum() / 1024 ** 2


            geodataframes[table_name] = {
                'gdf': gdf_converted,
                'memory_usage': mem_usage,
                'load_time_avg': load_time_avg,
                'conversion_time_avg': conversion_time_avg,
                'ignore_geometry': table.ignore_geometry
            }
        return geodataframes


def get_caller_directory(caller_frame: inspect) -> pathlib.Path:
    """
    Returns the file directory that calls this function.

    :param caller_frame: The frame that call the function.
    :return: A Pathlib.path object representing the file directory that called this function.
    """
    caller_file = inspect.getfile(caller_frame)
    return pathlib.Path(caller_file).resolve().parent


def get_caller_directory(caller_frame: inspect) -> pathlib.Path:
    """
    Returns the file directory that calls this function.

    :param caller_frame: The frame that call the function.
    :return: A Pathlib.path object representing the file directory that called this function.
    """
    caller_file = inspect.getfile(caller_frame)
    return pathlib.Path(caller_file).resolve().parent


def run_gui(folder_bdgd: str) -> None:
    caller_frame = inspect.currentframe().f_back
    caller_path = get_caller_directory(caller_frame)
    json_file = os.path.join(caller_path, "bdgd2dss.json")

    print(f"Base escolhida {folder_bdgd}")
    data = load_json(json_file=json_file)

    gui = GUI(folder_bdgd, data)
    gui.load_window()


def run(folder: Optional[str] = None, feeder: Optional[str] = None,  all_feeders: Optional[bool] = None, limit_ramal_30m: Optional[bool] = False) -> None:

    if feeder is None:
        all_feeders = True

    s = Sample()
    folder_bdgd = folder or s.mux_energia
    json_file_name = os.path.join(os.getcwd(), "bdgd2dss.json")

    json_data = JsonData(json_file_name)

    gdf_SSDMT, gdf_SSDBT = create_dfs_coords(folder_bdgd, feeder)
    df_coords = Coords.get_buscoords(gdf_SSDMT, gdf_SSDBT)
    create_output_feeder_coords(df_coords, feeder)

    geodataframes = json_data.create_geodataframes(folder_bdgd)

    for alimentador in geodataframes["CTMT"]['gdf']['COD_ID'].tolist():

        if alimentador == feeder or all_feeders == True:

            case = Case()
            case.dfs = geodataframes

            case.id = alimentador

            case.circuitos, aux  = Circuit.create_circuit_from_json(json_data.data, case.dfs['CTMT']['gdf'].query("COD_ID==@alimentador"))
            list_files_name = [aux]
            case.line_codes, aux= LineCode.create_linecode_from_json(json_data.data, case.dfs['SEGCON']['gdf'], alimentador)
            list_files_name.append(aux)

            for entity in ['SSDMT', 'UNSEMT', 'SSDBT', 'UNSEBT', 'RAMLIG']:

                if not case.dfs[entity]['gdf'].query("CTMT == @alimentador").empty:
                    if limit_ramal_30m == True:
                        case.lines_SSDMT, aux, aux_em = Line.create_line_from_json(json_data.data, case.dfs[entity]['gdf'].query("CTMT==@alimentador"), entity, ramal_30m = limit_ramal_30m)
                    else:
                        case.lines_SSDMT, aux, aux_em = Line.create_line_from_json(json_data.data, case.dfs[entity]['gdf'].query("CTMT==@alimentador"), entity)
                    list_files_name.append(aux)
                    if aux_em != "":
                        list_files_name.append(aux_em)
                else:
                    print(f'No {entity} elements found.\n')

            if not case.dfs['UNREMT']['gdf'].query("CTMT == @alimentador").empty:
                case.regcontrols,aux = RegControl.create_regcontrol_from_json(json_data.data, inner_entities_tables(case.dfs['EQRE']['gdf'], case.dfs['UNREMT']['gdf'].query("CTMT==@alimentador"),left_column='UN_RE', right_column='COD_ID'))
                list_files_name.append(aux)
            else:
                print("No RegControls found for this feeder.\n")

            case.transformers, aux= Transformer.create_transformer_from_json(json_data.data, inner_entities_tables(case.dfs['EQTRMT']['gdf'], case.dfs['UNTRMT']['gdf'].query("CTMT==@alimentador"), left_column='UNI_TR_MT', right_column='COD_ID'))
            list_files_name.append(aux)

            case.load_shapes, aux = LoadShape.create_loadshape_from_json(json_data.data, case.dfs['CRVCRG']['gdf'], alimentador)
            list_files_name.append(aux)

            case.loads, aux = Load.create_load_from_json(json_data.data, case.dfs['UCBT_tab']['gdf'].query("CTMT==@alimentador"),case.dfs['CRVCRG']['gdf'],'UCBT_tab')
            list_files_name.append(aux)

            case.loads, aux = Load.create_load_from_json(json_data.data, case.dfs['PIP']['gdf'].query("CTMT==@alimentador"),case.dfs['CRVCRG']['gdf'],'PIP')
            list_files_name.append(aux)

            if not case.dfs['UCMT_tab']['gdf'].query("CTMT == @alimentador").empty:
                case.loads, aux = Load.create_load_from_json(json_data.data, case.dfs['UCMT_tab']['gdf'].query("CTMT==@alimentador"),case.dfs['CRVCRG']['gdf'],'UCMT_tab')
                list_files_name.append(aux)
            else:
                print("Não há unidades consumidoras de média tensão neste alimentador")

            case.pvsystems, aux = PVsystem.create_pvsystem_from_json(json_data.data, case.dfs['UGBT_tab']['gdf'].query("CTMT==@alimentador"), 'UGBT_tab') #código adicionado por Mozart 07/07 às 14h
            list_files_name.append(aux)

            if not case.dfs['UGMT_tab']['gdf'].query("CTMT == @alimentador").empty: #adicionado por Mozart 17/07/2024 às 10:36
                case.pvsystems, aux = PVsystem.create_pvsystem_from_json(json_data.data, case.dfs['UGMT_tab']['gdf'].query("CTMT==@alimentador"), 'UGMT_tab') #código adicionado por Mozart 07/07 às 14h
                list_files_name.append(aux)
            else:
                print("Não há geração distribuída de média tensão neste alimentador. \n")

            case.output_master(list_files_name)
            case.create_outputs_masters(list_files_name)
