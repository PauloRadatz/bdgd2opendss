# -*- encoding: utf-8 -*-

import numpy as np
import pandas as pd
import shapely.geometry


def extract_shx(geo_df):
    lats = []
    lons = []
    names = []
    pac1 = []
    pac2 = []

    for feature, COD_ID, PAC_1, PAC_2 in zip(geo_df.geometry, geo_df.COD_ID, geo_df.PAC_1, geo_df.PAC_2):
        if isinstance(feature, shapely.geometry.linestring.LineString):
            linestrings = [feature]
        elif isinstance(feature, shapely.geometry.multilinestring.MultiLineString):
            linestrings = feature.geoms
        else:
            continue
        for linestring in linestrings:
            x, y = linestring.xy
            lats = np.append(lats, y)
            lons = np.append(lons, x)
            names = np.append(names, [COD_ID] * len(y))
            pac1 = np.append(pac1, [PAC_1] * len(y))
            pac2 = np.append(pac2, [PAC_2] * len(y))
            lats = np.append(lats, None)
            lons = np.append(lons, None)
            names = np.append(names, None)
            pac1 = np.append(pac1, None)
            pac2 = np.append(pac2, None)
    df = pd.DataFrame(list(zip(names, pac1, pac2, lats, lons))).rename(
            columns={0: 'COD_ID', 1: 'pac1', 2: 'pac2', 3: 'lat', 4: 'long'}).dropna().reset_index(drop=True)
    return df


def buses_coords(coords_shx, df_ssd):
    coords_shx['COD_ID'] = coords_shx['COD_ID'].astype(object)
    df_ssd['COD_ID'] = df_ssd['COD_ID'].astype(object)

    df1 = pd.merge(df_ssd[['COD_ID', 'PAC_1']], coords_shx.loc[0::2].drop('pac2', axis=1), on='COD_ID', how='left')
    df2 = pd.merge(df_ssd[['COD_ID', 'PAC_2']], coords_shx.loc[1::2].drop('pac1', axis=1), on='COD_ID', how='left')

    coords_shx = pd.concat([df1[['COD_ID', 'PAC_1', 'lat', 'long']].rename(columns={'PAC_1': 'PAC'}),
                            df2[['COD_ID', 'PAC_2', 'lat', 'long']].rename(columns={'PAC_2': 'PAC'})],
                           axis=0).reset_index(drop=True)

    # # Transformação para UTM, identificar zona
    # myProj = Proj('+proj=utm +zone=22 +south +ellps=WGS84 +datum=WGS84 +units=m +towgs84=-67.35,3.88,-38.22')
    # coords_shx['UTMx'], coords_shx['UTMy'] = myProj(coords_shx['long'],coords_shx['lat'])
    # # long, lat = myProj(UTMx, UTMy, inverse=True) # Para voltar para latlong
    # coords_shx['UTMx'] = round(coords_shx['UTMx'],3)
    # coords_shx['UTMy'] = round(coords_shx['UTMy'],3)

    return coords_shx


def get_buscoords(ssdmt, ssdbt):
    if not ssdmt.empty:
        coords_ssdmt_bdgd = extract_shx(ssdmt)
        buscoords_mt = buses_coords(coords_ssdmt_bdgd, ssdmt)
    else: #caso não exista sistema de média tensão, retorna apenas o sistema de baixa tensão.
        coords_ssdbt_bdgd = extract_shx(ssdbt)
        buscoords_bt = buses_coords(coords_ssdbt_bdgd, ssdbt)
        print("There's no SSDMT in this feeder.")
        return(buscoords_bt[['PAC', 'long', 'lat']])
    if not ssdbt.empty:
        coords_ssdbt_bdgd = extract_shx(ssdbt)
        buscoords_bt = buses_coords(coords_ssdbt_bdgd, ssdbt)
    else: #caso não exista o sistema de baixa tensão, retorna apenas o sistema de média tensão.
        print("There's no SSDBT in this feeder.")
        return(buscoords_mt[['PAC', 'long', 'lat']])
    buscoords = pd.concat([buscoords_mt[['PAC', 'long', 'lat']], buscoords_bt[['PAC', 'long', 'lat']]], axis=0).reset_index(drop=True)
    # return buscoords[['PAC', 'long', 'lat']].to_csv(path, index=False, header=False)
    return buscoords[['PAC', 'long', 'lat']]
