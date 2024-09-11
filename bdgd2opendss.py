import os
import pathlib
import bdgd2opendss as bdgd


if __name__ == '__main__':
    script_path = os.path.dirname(os.path.abspath(__file__))
    bdgd_file = str(pathlib.Path(script_path).joinpath("bdgd2opendss", "sample", "raw", "aneel", "CRELUZ-D_598_2022-12-31_V11_20230831-0921.gdb"))

    #feeder_list = bdgd.get_feeder_list(bdgd_file) #cria uma variável do tipo lista com os nomes dos alimentadores daquela bdgd
    #bdgd.export_feeder_list(feeder_list,feeder="1_3PAS_1") #exporta a lista criada para a pasta do alimentador selecionado

    bdgd.run(bdgd_file, feeder="1_3PAS_1")
    


    