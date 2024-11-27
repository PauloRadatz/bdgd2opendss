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
    bdgd_file_path = pathlib.Path(script_path, "bdgd2opendss", "sample", "raw", "aneel", "Creluz-D_598_2023-12-31_V11_20240715-1111.gdb")
    lst_feeders = ["1_3PAS_1"]

    # You can change setting below if needed
    
    # settings.intAdequarTensaoCargasBT = True
    # settings.intAdequarTensaoCargasMT = True
    # settings.intAdequarTensaoSuperior = False
    # settings.dblVPUMin = 0.8
    # settings.intAdequarModeloCarga = 1 #escolha 1 (models 2 e 3)//escolha 2 (models 1 e 1)// escolha 3 (models 3 e 3)
    # settings.intAdequarTapTrafo = False
    # settings.intNeutralizarRedeTerceiros = False
    # settings.intAdequarRamal = True
    # settings.intNeutralizarTrafoTerceiros = True
    
    bdgd.run(bdgd_file_path=bdgd_file_path, output_folder=pasta_de_saida, all_feeders=False,lst_feeders=lst_feeders)
