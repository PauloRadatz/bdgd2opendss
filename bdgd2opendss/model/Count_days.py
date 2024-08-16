# -*- encoding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import date
import holidays
import calendar


def get_holidays_br(year):
    holiday_list = []
    for holiday in holidays.Brazil(years=year).items():
        holiday_list.append(holiday)

    holidays_df = pd.DataFrame(holiday_list, columns=["date", "holiday"])
    holidays_df = holidays_df.sort_values('date')
    holidays_df['date'] = pd.to_datetime(holidays_df['date'])
#     holidays_df['date'] = pd.to_datetime(holidays_df['date'],format='%Y-%m-%d')
    holidays_df['mes']=holidays_df['date'].apply(lambda x: x.month)
    day_map = {'Sunday':'Domingo', 'Monday':'Segunda-feira', 'Tuesday':'Terça-feira', 'Wednesday':'Quarta-feira','Thursday':'Quinta-feira','Friday':'Sexta-feira','Saturday':'Sábado'}
    holidays_df['day_of_week'] = holidays_df['date'].dt.day_name().map(day_map)
    holidays_df['is_busday'] = holidays_df['day_of_week'].apply(lambda x: 0 if x=='Sábado' else 0 if x=='Domingo' else 1)
    return holidays_df

def count_days(day,month, year):
    if day =='sab':
        day_to_count = calendar.SATURDAY
        matrix = calendar.monthcalendar(year,month)
        num_days = sum(1 for x in matrix if x[day_to_count] != 0)
    if day =='dom':
        day_to_count = calendar.SUNDAY
        matrix = calendar.monthcalendar(year,month)
        num_days = sum(1 for x in matrix if x[day_to_count] != 0)
    return num_days

def count_busday(start, end):
    res = np.busday_count(start, end)
    return res

def count_day_type(ano):
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

    return df_days[['mes_ano', 'mes','holiday_busday_count', 'sab', 'dom', 'du']]
