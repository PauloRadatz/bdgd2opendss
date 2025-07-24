# -*- encoding: utf-8 -*-
import datetime

import pandas as pd
import numpy as np
from datetime import date
import holidays
import calendar
from bdgd2opendss.core.Utils import get_cod_year_bdgd
du = {}
sa = {}
do = {}
    
def calcula_carnaval(ano):
    """Calcula a data da Páscoa (domingo) para um determinado ano."""
    a = ano % 19
    b = ano // 100
    c = ano % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    p = (h + l - 7 * m + 114) % 31
    dia_pascoa = 1 + p
    x = (h + l - 7 * m + 114) // 31

    if x == 3:
        mes_pascoa = 3
    else:
        mes_pascoa = 4

    data_pascoa = date(ano,mes_pascoa,dia_pascoa)
    data_carnaval = data_pascoa - datetime.timedelta(days=47)
    data_corpus_christi = data_pascoa + datetime.timedelta(days=60)
    carnaval = (data_carnaval, 'Carnaval')
    corpus_christi = (data_corpus_christi, 'Corpus Christi')
    return(carnaval, corpus_christi)

def get_holidays_br(ano):
    holiday_list = []
    for holiday in holidays.Brazil(years=ano).items():
        holiday_list.append(holiday)
    carnaval, corpus_christi = calcula_carnaval(ano)
    holiday_list.append(carnaval)
    holiday_list.append(corpus_christi)
    holiday_list.sort()
    holidays_df = pd.DataFrame(holiday_list, columns=["date", "holiday"])
    holidays_df = holidays_df.sort_values('date')
    holidays_df['date'] = pd.to_datetime(holidays_df['date'])
#     holidays_df['date'] = pd.to_datetime(holidays_df['date'],format='%Y-%m-%d')
    holidays_df['mes']=holidays_df['date'].apply(lambda x: x.month)
    day_map = {'Sunday':'Domingo', 'Monday':'Segunda-feira', 'Tuesday':'Terça-feira', 'Wednesday':'Quarta-feira','Thursday':'Quinta-feira','Friday':'Sexta-feira','Saturday':'Sábado'}
    holidays_df['day_of_week'] = holidays_df['date'].dt.day_name().map(day_map)
    holidays_df['is_busday'] = holidays_df['day_of_week'].apply(lambda x: 0 if x=='Sábado' else 0 if x=='Domingo' else 1)
    return holidays_df

def count_days(day,month,ano):
    if day =='sab':
        day_to_count = calendar.SATURDAY
        matrix = calendar.monthcalendar(ano,month)
        num_days = sum(1 for x in matrix if x[day_to_count] != 0)
    if day =='dom':
        day_to_count = calendar.SUNDAY
        matrix = calendar.monthcalendar(ano,month)
        num_days = sum(1 for x in matrix if x[day_to_count] != 0)
    return num_days

def count_busday(start, end):
    res = np.busday_count(start, end)
    return res

def count_day_type(ano):
    global du
    global sa
    global do
    holidays_df = get_holidays_br(ano)
    df_days = holidays_df[['is_busday','mes']].groupby('mes').sum().reset_index().rename(columns={'is_busday':'holiday_busday_count'})

    df_aux = pd.DataFrame()

    for i in range(0,12):
        if i < 9:
            df_aux.loc[i,'mes_ano']=str(ano)+'-'+'0'+str(i+1)
            df_aux.loc[i,'mes'] = i+1
            df_aux.loc[i,'sab'] = count_days('sab',i+1, ano)
            df_aux.loc[i,'dom_i'] = count_days('dom',i+1, ano)
        else:
            df_aux.loc[i,'mes_ano']=str(ano)+'-'+str(i+1)
            df_aux.loc[i,'mes'] = i+1
            df_aux.loc[i,'sab'] = count_days('sab',i+1, ano)
            df_aux.loc[i,'dom_i'] = count_days('dom',i+1, ano)

    df_days = pd.merge(df_aux, df_days, on='mes', how='left')
    df_days['holiday_busday_count'].fillna(0, inplace=True )

    df_days['next_month'] = df_days['mes_ano'].shift(-1)
    df_days.iloc[-1,-1] = str(ano+1)+'-01'

    df_days['du_i'] = df_days.apply(lambda x: count_busday(x['mes_ano'], x['next_month']), axis=1)
    df_days['du'] = df_days['du_i']-df_days['holiday_busday_count']

    df_days['dom'] = df_days['dom_i']+df_days['holiday_busday_count']
    df_mes_str = df_days['mes'].apply(lambda x:f"{int(x):02d}")
    du = pd.Series(df_days['du'].values, index=df_mes_str).to_dict()
    sa = pd.Series(df_days['sab'].values, index=df_mes_str).to_dict()
    do = pd.Series(df_days['dom'].values, index=df_mes_str).to_dict()
    #return df_days[['mes_ano', 'mes','holiday_busday_count', 'sab', 'dom', 'du']]
    #return(du,sa,do)
    return(print(f'Contagem de dias para o ano de {ano} realizada'))


def return_day_type(tip_dia,mes):
    global du
    global sa
    global do
    if tip_dia == 'DU':
        return(du[mes])
    if tip_dia == 'SA':
        return(sa[mes])
    else:
        return(do[mes])
