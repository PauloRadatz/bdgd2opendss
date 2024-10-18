import bdgd2opendss as bdgd
from bdgd2opendss import settings
import pathlib
import os
import warnings

# Suppress RuntimeWarnings globally
warnings.filterwarnings("ignore", category=RuntimeWarning)

if __name__ == '__main__':

    script_path = os.path.dirname(os.path.abspath(__file__))
    bdgd_file_path = pathlib.Path(script_path, "bdgd2opendss", "sample", "raw", "aneel", "CRELUZ-D_598_2022-12-31_V11_20230831-0921.gdb")
    lstFeeders = ["1_3PAS_1"]

    # You can change setting below if needed
    settings.limitRamal30m = False

    bdgd.run(bdgd_file_path=bdgd_file_path, all_feeders=False, lstFeeders=lstFeeders)
