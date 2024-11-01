# -*- encoding: utf-8 -*-
import inspect
import os.path
import pathlib
from typing import List, Union, Optional

from bdgd2opendss.core.JsonData import JsonData
from bdgd2opendss.model.Case import Case

def get_caller_directory(caller_frame: inspect) -> pathlib.Path:
    """
    Returns the file directory that calls this function.

    :param caller_frame: The frame that call the function.
    :return: A Pathlib.path object representing the file directory that called this function.
    """
    caller_file = inspect.getfile(caller_frame)
    return pathlib.Path(caller_file).resolve().parent

def get_feeder_list(folder: str) -> List[str]:  # TODO is there a way to not load everything?
    folder_bdgd = folder
    json_file_name = os.path.join(os.getcwd(), "bdgd2dss.json")

    json_data = JsonData(json_file_name)
    geodataframes = json_data.create_geodataframes_lista_ctmt(folder_bdgd)

    return geodataframes["CTMT"]['gdf']['COD_ID'].tolist()

def export_feeder_list(feeder_list, feeder):

    if not os.path.exists("dss_models_output"):
        os.mkdir("dss_models_output")

    if not os.path.exists(f'dss_models_output/{feeder}'):
        os.mkdir(f'dss_models_output/{feeder}')

    output_directory = os.path.join(os.getcwd(), f'dss_models_output\{feeder}')

    path = os.path.join(output_directory, f'Alimentadores.txt')
    with open(path,'w') as output:
        for k in feeder_list:
            output.write(str(k)+"\n")
    return f'Lista de alimentadores criada em {path}'

def run(bdgd_file_path: Union[str, pathlib.Path],
        output_folder: Optional[Union[str, pathlib.Path]] = None,
        all_feeders: bool = True,
        lst_feeders: Optional[List[str]] = None) :

    #
    json_file_name = os.path.join(os.getcwd(), "bdgd2dss.json")
    json_obj = JsonData(json_file_name)
    geodataframes = json_obj.create_geodataframes(bdgd_file_path)

    # generates all feeders
    if all_feeders:

        for feeder in geodataframes["CTMT"]['gdf']['COD_ID'].tolist():

            case = Case(json_obj, bdgd_file_path, feeder, output_folder)
            case = Case(json_obj.data, geodataframes, bdgd_file_path, feeder, output_folder)

    else :

        for feeder in lst_feeders:

            # verifies if the feeder exists
            if feeder not in geodataframes["CTMT"]['gdf']['COD_ID'].tolist() :
                print(f"\nFeeder: {feeder} not found in CTMT.")
                continue

            case = Case(json_obj.data, geodataframes, bdgd_file_path, feeder, output_folder)
