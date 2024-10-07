import os
import pathlib
import bdgd2opendss as bdgd
from bdgd2opendss import Parameters

if __name__ == '__main__':

    #feeder_list = bdgd.get_feeder_list(bdgd_file) #cria uma variável do tipo lista com os nomes dos alimentadores daquela bdgd
    #bdgd.export_feeder_list(feeder_list,feeder="1_3PAS_1") #exporta a lista criada para a pasta do alimentador selecionado

    script_path = os.path.dirname(os.path.abspath(__file__))

    bdgdPath = str(pathlib.Path(script_path).joinpath( "CRELUZ-D_598_2022-12-31_V11_20230831-0921.gdb"))
    alimentador = "1_REDE2_1" # "1_3PAS_1" # "1_REDE2_1"

    # OLD CODE - DEBUG ONLY
    # bdgdPath = str(pathlib.Path(script_path).joinpath( "CPFL_Paulista_63_2023-12-31_V11_20240508.gdb"))
    # alimentador = "ABR07"

    # PARAMETERS
    # folder_bdgd
    # alimTest
    # OPTIONAL PARAMETERS
    # allFeeders = False  # generates all feeders
    # limitRamal30m = True  # limits ramal to 30m
    # ger4fios = True  # generates with Neutral
    # gerCapacitors = False  # generates capacitors banks
    # loadModel = "ANEEL"  # loadModel ANEEL (e.g 2 loads for each load), model8
    # EXAMPLE
    # par = Parameters(bdgdPath,alimentador)

    par = Parameters(bdgdPath, alimentador, False, True, True,  False, "ANEEL")

    bdgd.run(par)
