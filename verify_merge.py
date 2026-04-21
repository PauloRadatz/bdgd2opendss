import bdgd2opendss as bdgd
from bdgd2opendss import settings
import os
import pathlib

# Configure paths
bdgd_file_path = r"e:\VE_PETROBRAS\DADOS_BDGD\Enel_SP_390_2024-12-31_V11_20250926-0906.gdb"
lst_feeders = ['LAP0115']
output_folder = "test_merge_output"

# Enable merging
settings.blnMergeSeriesLines = True
settings.TipoBDGD = False # Adjust if needed, most samples are public structure
settings.intAdequarModeloCarga = 1

print(f"Starting conversion for feeder {lst_feeders} with blnMergeSeriesLines={settings.blnMergeSeriesLines}")

try:
    bdgd.run(bdgd_file_path=bdgd_file_path, all_feeders=False, lst_feeders=lst_feeders, output_folder=output_folder)
    print("Conversion completed successfully.")
except Exception as e:
    print(f"An error occurred: {e}")
