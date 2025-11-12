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
    bdgd_file_path = pathlib.Path(script_path, "bdgd2opendss", "sample", "raw", "aneel", r"C:\Users\mozar\Downloads\base_Creluz_BDGD_2023")
    bdgd_file_path = r"C:\PauloRadatz\GitHub\bdgd2opendss\bdgd2opendss\sample\raw\aneel\Creluz-D_598_2023-12-31_V11_20240715-1111.gdb\Creluz-D_598_2023-12-31_V11_20240715-1111.gdb"
    bdgd_file_path = r"C:\PauloRadatz\GitHub\ProjetoPerdas\BDGD\base_Creluz_BDGD_2023"
    lst_feeders = ['1_3PAS_1']#lista
    #pastadesaida = r"C:\Users\mozar\OneDrive\Desktop\creluz\casos_bdgd2opendss_todos"

    # You can change setting below if needed
    settings.TabelaPT = True # - Defina True se quiser gerar as tabelas para os cálculos de Perdas Técnicas (AuxBTNT,AuxMTNT e CircMT)
    #settings.intUsaTrafoABNT = True # - Usa as perdas dos transformadores da ABNT 5440
    #settings.intAdequarTensaoCargasMT = True # - adequar tensão mínima das cargas MT (0.93)
    #settings.intAdequarTensaoCargasBT = True # - adequar tensão mínima das cargas BT (0.92)
    #settings.intAdequarTensaoSuperior = True # - limitar tensão máxima de barras
    #settings.intAdequarRamal = True # - limitar ramal em 30 metros
    settings.intAdequarModeloCarga = 1 # escolha 1 (models 2 e 3)//escolha 2 (models 1 e 1)// escolha 3 (models 3 e 3)
    #settings.intAdequarTapTrafo = True # - utilizar os taps dos trafos
    #settings.intAdequarPotenciaCarga = True # - Adequa potência das cargas BT a carga do transformador conectado
    #settings.intAdequarTrafoVazio = True # - comentar trafos a vazio
    #settings.intNeutralizarTrafoTerceiros = True # - neutralizar trafos de terceiro
    #settings.intNeutralizarRedeTerceiros = True # - neutralizar redes de terceiros
    settings.dblVPUMin = 0.6 # - tensão mínima das cargas
    # settings.cbMeterComplete = True # - (True) Criar medidores de energia nos transformadores e barramento/ (False) Cria só no barramento
    #settings.CRVCRG96pts = True # Defina True se quiser gerar o arquivo de curva de carga com 96 pts (diferente do geoperdas)
    bdgd.run(bdgd_file_path=bdgd_file_path,all_feeders=False, lst_feeders=lst_feeders)
    #bdgd.run_errors(bdgd_file_path=bdgd_file_path,output_folder=pastadesaida,lista_ctmt=lista)
