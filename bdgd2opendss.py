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
    
    settings.intAdequarTensaoCargasBT = True # - adequar tensão mínima das cargas BT (0.92)
    settings.intAdequarTensaoCargasMT = True # - adequar tensão mínima das cargas MT (0.93)
    settings.intAdequarTensaoSuperior = True # - limitar tensão máxima de barras 
    # settings.dblVPUMin = 0.8 # - tensão mínima das cargas
    settings.intAdequarModeloCarga = 1 # escolha 1 (models 2 e 3)//escolha 2 (models 1 e 1)// escolha 3 (models 3 e 3)
    settings.intAdequarTapTrafo = True # - utilizar os taps dos trafos
    # settings.intNeutralizarRedeTerceiros = True # - neutralizar redes de terceiros 
    # settings.intAdequarRamal = True # - limitar ramal em 30 metros
    # settings.intNeutralizarTrafoTerceiros = True # - neutralizar trafos de terceiro
    # settings.intAdequarTrafoVazio = True # - comentar trafos a vazio 
    # settings.intAdequarPotenciaCarga = True # - Adequa potência das cargas BT a carga do transformador conectado
    # settings.intUsaTrafoABNT = False # - Usa as perdas dos transformadores da ABNT 5440
    # settings.cbMeterComplete = False # - (True) Criar medidores de energia nos transformadores MTMT e barramento/ (False) Cria só no barramento
    bdgd.run(bdgd_file_path=bdgd_file_path, all_feeders=False,lst_feeders=lst_feeders)
