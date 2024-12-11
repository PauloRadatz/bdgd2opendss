import pandas as pd
import geopandas as gpd
from typing import Optional
from bdgd2opendss.model.Circuit import Circuit
from bdgd2opendss.model.Transformer import Transformer
from bdgd2opendss.core.Settings import settings
from bdgd2opendss.core.Utils import get_cod_year_bdgd, create_output_file

from dataclasses import dataclass

def create_energymeters(dataframe: gpd.geodataframe.GeoDataFrame, df_trafo, feeder, pastadesaida):
    alimentador = feeder
    energymeters = []
    df_aux_tramo = dataframe
    df_aux_trafo = df_trafo
    if settings.cbMeterComplete:
        cont = 0
        while cont < len(df_aux_trafo):
            df_aux_trafo['kv1'] = [Transformer.dict_kv_pri().get(cod_id, 0) for cod_id in df_aux_trafo['COD_ID']]
            df_aux_trafo['kv2'] = [Transformer.dict_kv().get(cod_id[:-1], 0) for cod_id in df_aux_trafo['COD_ID']]
            medidor = name_em(df_aux_trafo['ELEM'].tolist()[cont],df_aux_trafo['COD_ID'].tolist()[cont],'completo',df_aux_trafo['kv1'].tolist()[cont],df_aux_trafo['kv2'].tolist()[cont])
            elemento = elem_em(df_aux_trafo['ELEM'].tolist()[cont],df_aux_trafo['COD_ID'].tolist()[cont])
            if medidor:
                energymeters.append(f'New "Energymeter.{medidor}" element="{elemento}" terminal=1')
            cont += 1

        df_em_barramento = df_aux_tramo.loc[(df_aux_tramo['PAC_1']==Circuit.pac_ctmt()) | (df_aux_tramo['PAC_2']==Circuit.pac_ctmt())]
        cont = 0
        while cont < len(df_em_barramento):
            medidor = name_em(df_em_barramento['ELEM'].tolist()[cont],df_em_barramento['COD_ID'].tolist()[cont],'barramento',Circuit.kvbase())
            elemento = elem_em(df_em_barramento['ELEM'].tolist()[cont],df_em_barramento['COD_ID'].tolist()[cont])
            energymeters.append(f'New "Energymeter.{medidor}" element="{elemento}" terminal=1')
            cont += 1
    else:
        df_em_barramento = df_aux_tramo.loc[(df_aux_tramo['PAC_1']==Circuit.pac_ctmt()) | (df_aux_tramo['PAC_2']==Circuit.pac_ctmt())]
        cont = 0
        while cont < len(df_em_barramento):
            medidor = name_em(df_em_barramento['ELEM'].tolist()[cont],df_em_barramento['COD_ID'].tolist()[cont],'barramento',Circuit.kvbase())
            elemento = elem_em(df_em_barramento['ELEM'].tolist()[cont],df_em_barramento['COD_ID'].tolist()[cont])
            energymeters.append(f'New "Energymeter.{medidor}" element="{elemento}" terminal=1')
            cont += 1
    
    file_name = create_output_file(energymeters, "Medidores", feeder=alimentador, output_folder=pastadesaida)
    
    return(file_name)
    
def name_em(elem,nome,tipo,kv,kv2:Optional[float] = None):

    if kv <= 1:
        prefix = 'B--'
    elif kv <= 25:
        prefix = 'A4-'
    else:
        prefix = 'A3a'
    if kv2 is not None:
        if kv2 <= 1:
            prefix2 = 'B--'
        elif kv2 <= 25:
            prefix2 = 'A4-'
        else:
            prefix2 = 'A3a'
    else:
        ...
    if tipo == 'barramento':
        return(f'Bus{prefix}_{get_cod_year_bdgd()}_{elem}_{nome}')
    elif kv2 > 1:
        return(f'L{prefix}{prefix2}_{get_cod_year_bdgd()}_{elem}_{nome}')
    elif kv2 > 1 or (kv2 <= 1 and kv > 25):
        return(f'T{prefix}{prefix2}_{get_cod_year_bdgd()}_{elem}_{nome}')
    else:
        return(False)
    
def elem_em(elem,nome):
    if elem == 'SEGMMT':
        return(f'Line.SMT_{nome}')
    elif elem == 'SEGMBT':
        return(f'Line.SBT_{nome}')
    elif elem == 'RML':
        return(f'Line.RBT_{nome}')
    elif elem == 'CHVMT':
        return(f'Line.CMT_{nome}')
    elif elem == 'CHVBT':
        return(f'Line.CBT_{nome}')
    elif elem == 'TRAFO':
        return(f'Transformer.TRF_{nome}')
    else:
        return(f'Transformer.REG_{nome}')

