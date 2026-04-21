import bdgd2opendss as bdgd
from bdgd2opendss import settings
import pathlib
import os
import warnings

# Suppress RuntimeWarnings globally
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

if __name__ == '__main__':
    script_path = os.path.dirname(os.path.abspath(__file__))
    bdgd_file_path = r"E:\VE_PETROBRAS\DADOS_BDGD\Enel_SP_390_2024-12-31_V11_20250926-0906.gdb\Enel_SP_390_2024-12-31_V11_20250926-0906.gdb"
    lst_feeders = ["LAP0115"]
    output_folder = "E:\VE_PETROBRAS\DADOS_BDGD\Enel_SP_390_2024-12-31_V11_20250926-0906.gdb\TEST_8"
    settings.blnMergeSeriesLines = True
    settings.blnPruneDanglingBranches = True
    settings.blnBalancCargasBT = True

    bdgd.run(bdgd_file_path=bdgd_file_path, output_folder=output_folder, lst_feeders=lst_feeders, all_feeders=False)
    