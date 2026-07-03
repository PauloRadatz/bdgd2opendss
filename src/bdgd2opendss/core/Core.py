# -*- encoding: utf-8 -*-
import inspect
import os.path
import pathlib
import copy
from typing import List, Union, Optional
from tqdm import tqdm
from bdgd2opendss.core.JsonData import JsonData
from bdgd2opendss.core import Utils
from bdgd2opendss.model.Case import Case
from bdgd2opendss.model import BusCoords
from bdgd2opendss.model.validador_bdgd import (
    ValidadorBDGD,
    _log_verification_failure,
    _report_verification,
    _verification_log_path,
) #etapa17
from bdgd2opendss.core.Settings import settings
from bdgd2opendss.config.paths import bdgd2dss_json, bdgd2dss_private_json, bdgd2dss_error_json, bdgd2dss_error_private_json

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
            return "privada"
        elif ext == '.gdbtable' or ext == '.gdbtablx' or ext == '.gdbindexes':
            return "publica"

def run(bdgd_file_path: Union[str, pathlib.Path],
        output_folder: Union[str, pathlib.Path],
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

def _handle_uncaught_verification_failure(validation: ValidadorBDGD, phase: str, exc: Exception) -> None:
    if not getattr(validation, "cod_base", None):
        validation.cod_base = validation._cod_base_from_df()
    log_path = _verification_log_path(validation.output_folder, validation.feeders, validation.cod_base)
    validation._record_verification_failure(
        log_path, phase, phase, f"Falha nao tratada em {phase}", exc,
    )
    _report_verification(phase, f"FALHA: {exc}")


def _verification_summary(validation: ValidadorBDGD, phase_label: str) -> None:
    if validation._verification_failures:
        log_path = validation._verification_log_path()
        _report_verification(
            phase_label,
            f"Concluida com {len(validation._verification_failures)} falha(s) - ver {log_path}",
        )
    else:
        _report_verification(phase_label, "Validacao concluida")


def _create_validation_coords(bdgd_file_path, feeder, output_folder):
    gdf_SSDMT, gdf_SSDBT, gdf_UCBT, gdf_UCMT = Utils.create_dfs_coords(bdgd_file_path, feeder, print_status=False)
    df_coords = BusCoords.get_buscoords(gdf_SSDMT, gdf_SSDBT, gdf_UCBT, gdf_UCMT)
    if df_coords is None:
        return None
    Utils.create_output_feeder_coords(df_coords, feeder=feeder, output_folder=output_folder, print_status=False)
    output_directory = Utils.create_output_folder(feeder=feeder, output_folder=output_folder)
    return os.path.join(output_directory, "buscoords.csv")


def _create_validation_coords_for_feeders(bdgd_file_path, feeders, output_folder):
    print("criando coordenadas...")
    coords_paths = {}
    for feeder in tqdm(feeders, desc="Coordenadas", unit=" alimentador", ncols=100):
        generated_path = _create_validation_coords(bdgd_file_path, feeder, output_folder)
        if generated_path:
            coords_paths[feeder] = generated_path
    if coords_paths:
        print("Arquivo buscoords.csv criado")
    return coords_paths


def verificacao_bdgd(bdgd_file_path: Union[str, pathlib.Path],
            output_folder: Union[str, pathlib.Path],
            all_feeders: Optional[bool] = True, lst_feeders: Optional[list] = None,
            export_figs: Optional[bool] = False):
    tipo = bdgd_type(bdgd_file_path)
    if tipo:
        _report_verification("inicio", f"BDGD {tipo} detectada")

    if settings.TipoBDGD:
        #json_file_name = bdgd2dss_private_json
        json_file_name = bdgd2dss_error_private_json
    else:
        #json_file_name = bdgd2dss_json
        json_file_name = bdgd2dss_error_json
        bdgd_pub = ['UCBT_tab','UCMT_tab','UGBT_tab','UGMT_tab']

    json_obj = JsonData(json_file_name)

    _report_verification("carga", "Carregando tabelas da BDGD...")
    geodataframe,tables = json_obj.create_geodataframe_errors(bdgd_file_path)
    _report_verification("carga", "Tabelas carregadas")

    if not settings.TipoBDGD: #se for BDGD pública
        for key in bdgd_pub:
            geodataframe[key[0:4]] = geodataframe.pop(key)

    if all_feeders:
        validation_path_coords = None
        if export_figs:
            coords_by_feeder = _create_validation_coords_for_feeders(
                bdgd_file_path,
                geodataframe["CTMT"]["COD_ID"].tolist(),
                output_folder,
            )
            if coords_by_feeder:
                os.makedirs(output_folder, exist_ok=True)
                validation_path_coords = os.path.join(output_folder, "buscoords.csv")
                with open(validation_path_coords, "w", encoding="utf-8") as output:
                    for index, coords_path in enumerate(coords_by_feeder.values()):
                        with open(coords_path, encoding="utf-8") as input_file:
                            lines = input_file.readlines()
                        output.writelines(lines if index == 0 else lines[1:])

        validation = ValidadorBDGD(df=geodataframe,output_folder=output_folder,tables=tables,path_coords=validation_path_coords)

        _report_verification("scan", "Iniciando pre-validacao (scan)...")
        try:
            validation.scan_bdgd()
        except Exception as exc:
            _handle_uncaught_verification_failure(validation, "scan", exc)
        _report_verification("validacao", "Iniciando validacao Etapa 17...")
        try:
            validation.run_validation()
        except Exception as exc:
            _handle_uncaught_verification_failure(validation, "etapa17", exc)
        _verification_summary(validation, "validacao")

    else:
        gdf = copy.deepcopy(geodataframe)
        lst_entity = ['BASE','UNTRAT','SEGCON','CRVCRG','EQTRMT','EQRE']
        keys = [x for x in gdf.keys() if x not in lst_entity]

        feeders = lst_feeders or []
        coords_by_feeder = {}
        if export_figs:
            coords_by_feeder = _create_validation_coords_for_feeders(bdgd_file_path, feeders, output_folder)
        feeder_iterator = tqdm(feeders, desc="Alimentadores", unit=" alimentador", ncols=100)
        for feeder in feeder_iterator:
            feeder_iterator.set_description(f"Alimentador: {feeder}")
            _report_verification("alimentador", f"Processando {feeder}")
            validation_path_coords = coords_by_feeder.get(feeder)
            alimentador = feeder
            for key in keys: #resetar os índices será ?
                if key == 'CTMT':
                    gdf['CTMT'] = geodataframe['CTMT'][geodataframe['CTMT']['COD_ID'] == feeder].reset_index(drop=True)
                else:
                    gdf[key] = geodataframe[key].query("CTMT == @alimentador").reset_index(drop=True)

            validation = ValidadorBDGD(df=gdf,output_folder=output_folder,tables=tables,feeders=feeder,path_coords=validation_path_coords)

            _report_verification("scan", f"Iniciando pre-validacao (scan) para {feeder}...")
            try:
                validation.scan_bdgd()
            except Exception as exc:
                _handle_uncaught_verification_failure(validation, "scan", exc)
            _report_verification("validacao", f"Iniciando validacao Etapa 17 para {feeder}...")
            try:
                validation.run_validation()
            except Exception as exc:
                _handle_uncaught_verification_failure(validation, "etapa17", exc)
            _verification_summary(validation, f"validacao/{feeder}")



