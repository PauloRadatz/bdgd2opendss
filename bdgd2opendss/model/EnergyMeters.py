import pandas as pd
import geopandas as gpd
from typing import Optional
from bdgd2opendss.model.Circuit import Circuit
from bdgd2opendss.model.Transformer import Transformer
from bdgd2opendss.core.Settings import settings
from bdgd2opendss.core.Utils import get_cod_year_bdgd, create_output_file,inner_entities_tables,adapt_regulators_names

from dataclasses import dataclass


def create_aux_tramo(dataframe: gpd.geodataframe.GeoDataFrame, feeder): #tabela auxiliar para os energymeters
    alimentador = feeder
    df_trafo = merge_df_aux_tr(dataframe['EQTRMT']['gdf'], dataframe['UNTRMT']['gdf'].query("CTMT==@alimentador"),
                            left_column='UNI_TR_MT', right_column='COD_ID')
    adapt_regulators_names(df_trafo,'transformer')
    df_aux_ssdmt = dataframe['SSDMT']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
    df_aux_ssdmt['ELEM'] = 'SEGMMT'
    df_aux_ssdbt = dataframe['SSDBT']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
    df_aux_ssdbt['ELEM'] = 'SEGMBT'
    df_aux_ramalig = dataframe['RAMLIG']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
    df_aux_ramalig['ELEM'] = 'RML'
    df_aux_unsemt = dataframe['UNSEMT']['gdf'].query("CTMT == @alimentador" and "P_N_OPE == 'F'")[['COD_ID','CTMT','PAC_1','PAC_2']]
    df_aux_unsemt['ELEM'] = 'CHVMT'
    df_aux_unsebt = dataframe['UNSEBT']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
    df_aux_unsebt['ELEM'] = 'CHVBT'
    df_aux_trafo = df_trafo[['COD_ID','CTMT','PAC_1','PAC_2']]
    df_aux_trafo['ELEM'] = 'TRAFO'
    df_aux_regul = dataframe['UNREMT']['gdf'].query("CTMT == @alimentador")[['COD_ID','CTMT','PAC_1','PAC_2']]
    df_aux_regul['ELEM'] = 'REGUL'
    df_aux_tramo = pd.concat([df_aux_ssdmt,df_aux_ssdbt,df_aux_ramalig,df_aux_unsemt,df_aux_unsebt,df_aux_trafo,df_aux_regul], ignore_index=True)
    return(df_aux_tramo,df_aux_trafo)
    
def create_energymeters(dataframe: gpd.geodataframe.GeoDataFrame, feeder, pastadesaida):
    alimentador = feeder
    energymeters = []
    df_aux_tramo,df_aux_trafo = create_aux_tramo(dataframe=dataframe, feeder=alimentador)
    if settings.cbMeterComplete:
        cont = 0
        while cont < len(df_aux_trafo):
            df_aux_trafo['kv2'] = [Transformer.dict_kv().get(cod_id, 0) for cod_id in df_aux_trafo['COD_ID']]
            medidor = name_em(df_aux_trafo['ELEM'].tolist()[cont],df_aux_trafo['COD_ID'].tolist()[cont],'completo',Circuit.kvbase(),df_aux_trafo['kv2'].tolist()[cont])
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

def merge_df_aux_tr(dataframe_1,dataframe_2,right_column,left_column):

    merged_dfs = pd.merge(dataframe_1, dataframe_2, left_on=left_column, right_on=right_column, how='inner')

    for column in merged_dfs.columns:
        if column.endswith('_x'):
            merged_dfs.drop(columns=column, inplace=True)
        elif column.endswith('_y'):
            new_column_name = column[:-2]  # Remove the '_df2' suffix
            merged_dfs.rename(columns={column: new_column_name}, inplace=True)
    
    return(merged_dfs)