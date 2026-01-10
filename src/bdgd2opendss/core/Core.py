# -*- encoding: utf-8 -*-
import inspect
import os.path
import pathlib
import copy
from typing import List, Union, Optional
from bdgd2opendss.core.JsonData import JsonData
from bdgd2opendss.model.Case import Case
from bdgd2opendss.model.validador_bdgd import ValidadorBDGD #etapa17
from bdgd2opendss.core.Settings import settings
from bdgd2opendss.config.paths import bdgd2dss_json, bdgd2dss_private_json

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

    if settings.TipoBDGD:
        json_file_name = bdgd2dss_private_json
    else:
        json_file_name = bdgd2dss_json

    json_obj = JsonData(json_file_name)
    geodataframes = json_obj.create_geodataframes_lista_ctmt(folder_bdgd)

    return geodataframes["CTMT"]['gdf']['COD_ID'].tolist()

def export_feeder_list(feeder_list, feeder):

    if not os.path.exists("dss_models_output"):
        os.mkdir("dss_models_output")

    if not os.path.exists(f'dss_models_output/{feeder}'):
        os.mkdir(f'dss_models_output/{feeder}')
        
    output_directory = os.path.join(os.getcwd(), f'dss_models_output/{feeder}')

    path = os.path.join(output_directory, f'Alimentadores.txt')
    with open(path,'w') as output:
        for k in feeder_list:
            output.write(str(k)+"\n")
    return f'Lista de alimentadores criada em {path}'

def bdgd_type(path):
    arquivos = os.listdir(path)
    for item in arquivos:
        x,ext = os.path.splitext(item)
        if ext == '.dbf' or ext == '.shp' or ext == '.shx' or ext == '.prj':
            settings.TipoBDGD = True
            break
        elif ext == '.gdbtable' or ext == '.gdbtablx' or ext == '.gdbindexes':
            break
        
def run(bdgd_file_path: Union[str, pathlib.Path],
        output_folder: Optional[Union[str, pathlib.Path]] = None,
        all_feeders: bool = True,
        lst_feeders: Optional[List[str]] = None) :

    bdgd_type(bdgd_file_path) #define automaticamente se a bdgd é pública ou privada
    #
    if settings.TipoBDGD:
        json_file_name = bdgd2dss_private_json
    else:
        json_file_name = bdgd2dss_json
    json_obj = JsonData(json_file_name)
    geodataframes = json_obj.create_geodataframes(bdgd_file_path)
    #TODO implementar aqui o ValidadorBDGD.run(geodataframes,output_file)
    # generates all feeders
    if all_feeders:

        for feeder in geodataframes["CTMT"]['gdf']['COD_ID'].tolist():

            case = Case(json_obj.data, geodataframes, bdgd_file_path, feeder, output_folder)
            case.PopulaCase()

    else :

        for feeder in lst_feeders:

            # verifies if the feeder exists
            if feeder not in geodataframes["CTMT"]['gdf']['COD_ID'].tolist() :
                print(f"\nFeeder: {feeder} not found in CTMT.")
                continue

            case = Case(json_obj.data, geodataframes, bdgd_file_path, feeder, output_folder)
            case.PopulaCase()

def verificacao_bdgd(bdgd_file_path: Union[str, pathlib.Path], all_feeders: Optional[bool] = True, lst_feeders: Optional[list] = None,
            output_folder: Optional[Union[str, pathlib.Path]] = None):
    bdgd_type(bdgd_file_path)

    if settings.TipoBDGD:
        #json_file_name = bdgd2dss_private_json
        json_file_name = r"C:/Users/mozar/OneDrive/Documentos/GitHub/bdgd2opendss/src/bdgd2opendss/config/bdgd2dss_error_private.json"
    else:
        #json_file_name = bdgd2dss_json
        json_file_name = r"C:/Users/mozar/OneDrive/Documentos/GitHub/bdgd2opendss/src/bdgd2opendss/config/bdgd2dss_error.json"
        bdgd_pub = ['UCBT_tab','UCMT_tab','UGBT_tab','UGMT_tab']

    json_obj = JsonData(json_file_name)

    geodataframe,tables = json_obj.create_geodataframe_errors(bdgd_file_path)
    
    if not settings.TipoBDGD: #se for BDGD pública
        for key in bdgd_pub:
            geodataframe[key[0:4]] = geodataframe.pop(key)

    if all_feeders:
        validation = ValidadorBDGD(df=geodataframe,output_folder=output_folder,tables=tables)
        
        validation.run_validation()
    else:
        gdf = copy.deepcopy(geodataframe)
        lst_entity = ['BASE','UNTRAT','SEGCON','CRVCRG','EQTRMT','EQRE']
        keys = [x for x in gdf.keys() if x not in lst_entity]

        for feeder in lst_feeders:
            alimentador = feeder
            for key in keys: #resetar os índices será ?
                if key == 'CTMT':
                    gdf['CTMT'] = geodataframe['CTMT'][geodataframe['CTMT']['COD_ID'] == feeder].reset_index(drop=True)
                else:
                    gdf[key] = geodataframe[key].query("CTMT == @alimentador").reset_index(drop=True)
    
            validation = ValidadorBDGD(df=gdf,output_folder=output_folder,tables=tables,feeders=feeder)
            
            validation.run_validation()

            