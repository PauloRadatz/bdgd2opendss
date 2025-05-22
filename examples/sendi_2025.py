import bdgd2opendss as bdgd
import pathlib
import os
import warnings

# Suppress RuntimeWarnings globally
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

if __name__ == '__main__':
    script_path = os.path.dirname(os.path.abspath(__file__))
    bdgd_file_path = pathlib.Path(script_path, "../bdgd2opendss", "sample", "raw", "aneel",
                                  "Creluz-D_598_2023-12-31_V11_20240715-1111.gdb")
    lst_feeders = ["1_3PAS_1"]
    output_folder = "C:\SENDI\DSS"

    bdgd.run(bdgd_file_path=bdgd_file_path, output_folder=output_folder, lst_feeders=lst_feeders, all_feeders=False)
