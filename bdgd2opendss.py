import bdgd2opendss as bdgd
from bdgd2opendss import settings
import pathlib
import os
import warnings

# Suppress RuntimeWarnings globally
warnings.filterwarnings("ignore", category=RuntimeWarning)

if __name__ == '__main__':

    # CRELUZ-D_598_2022-12-31_V11_20230831-0921.gdb
    script_path = os.path.dirname(os.path.abspath(__file__))
    bdgd_file_path = pathlib.Path(script_path, "bdgd2opendss", "sample", "raw", "aneel", "CEEE_Equatorial_5707_2023-12-31_V11_20240423-1557.gdb")
    lst_feeders = ["1_3PAS_1"]
    pasta_de_saida = "" #You can choose a output folder here for dss files generated. If you don't choose, there's a default folder.

    # You can change setting below if needed
    settings.limitRamal30m = False

    bdgd.run(bdgd_file_path=bdgd_file_path, output_folder=pasta_de_saida, all_feeders=False, lst_feeders=lst_feeders)

    # bdgd_file_path = r"F:\DropboxZecao\Dropbox\0CEMIG\0_BDGDs\CEEE_Equatorial_5707_2023-12-31_V11_20240423-1557.gdb"
    # lst = bdgd.get_feeder_list(bdgd_file_path)
#
