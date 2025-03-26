import bdgd2opendss as bdgd
from bdgd2opendss import settings
import pathlib
import os
import warnings

# Suppress RuntimeWarnings globally
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

if __name__ == '__main__':
    #Enel_CE_39_2023-12-31_V11_20240426-1710.gdb
    #"C:\Users\mozar\OneDrive\Desktop\mestradoproflucas\BDGD\Creluz-D_598_2023-12-31_V11_20240715-1111.gdb"
    lista = bdgd.get_feeder_list(r"C:\Users\mozar\Downloads\Coprel_2351_2023-12-31_V11_20240610-1508.gdb")
    lista_2 = []
    for feeder in lista:
        if feeder not in os.listdir(r"C:\Users\mozar\OneDrive\Desktop\bdgd2opendss\dss_models_output"):
            lista_2.append(feeder)
    # lista = ["32067020"] #alimentador não tem nada, apenas SSDMT e CHVMT#["1_3PAS_1"] #["1_ALPE_1","1_BOES_1","1_MIRA_1","1_REDE2_1","1_SE001_1","2_PAL2_1","17_SE001_1","18_SE001_1","22_SE001_1","25_SE001_1","30_SE001_1","9718_9718_1"]
    script_path = os.path.dirname(os.path.abspath(__file__))
    bdgd_file_path = pathlib.Path(script_path, "bdgd2opendss", "sample", "raw", "aneel", r"C:\Users\mozar\OneDrive\Desktop\mestradoproflucas\BDGD\Creluz-D_598_2023-12-31_V11_20240715-1111.gdb")
    lst_feeders = ["17_SE001_1"]#lista
    pastadesaida = None #r"C:\Users\mozar\OneDrive\Desktop\bdgd2opendss\dss_models_output" 
    #pastadesaida = r"C:\Users\mozar\OneDrive\Desktop\creluz\casos_bdgd2opendss_todos"
    
    # You can change setting below if needed

    #settings.TipoBDGD = True # - Defina True para BDGD privada da distribuidora e False para BDGD Pública disponibilizada pela ANEEL
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
    bdgd.run(bdgd_file_path=bdgd_file_path,output_folder=pastadesaida, all_feeders=False, lst_feeders=lst_feeders)
    #bdgd.run_errors(bdgd_file_path=bdgd_file_path,output_folder=pastadesaida,lista_ctmt=lista)