import json
import time
import os
import geopandas as gpd
import pandas as pd

# import os
# from typing import Optional

class Table:
    def __init__(self, name, columns, data_types, ignore_geometry_):
        self.name = name
        self.columns = columns
        self.data_types = data_types
        self.ignore_geometry = ignore_geometry_

    def __str__(self):
        return f"Table(name={self.name}, columns={self.columns}, data_types={self.data_types}, " \
               f"ignore_geometry={self.ignore_geometry})"

class JsonData:
    def __init__(self, file_name):
        """
        Inicializa a classe JsonData com o nome do arquivo de entrada.
        :param file_name: Nome do arquivo JSON de entrada.
        """
        self.data = self._read_json_file(file_name)
        self.tables = self._create_tables()

    @staticmethod
    def _read_json_file(file_name):
        """
        Lê o arquivo JSON fornecido e retorna o conteúdo como um objeto Python.
        :param file_name: Nome do arquivo JSON de entrada.
        :return: Objeto Python contendo o conteúdo do arquivo JSON.
        """
        with open(file_name, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data

    def _create_tables(self):
        """
        Cria um dicionário de tabelas a partir dos dados carregados do arquivo JSON.
        :return: Dicionário contendo informações das tabelas a serem processadas.
        """
        return {
            table_name: Table(
                table_name,
                table_data["columns"],
                table_data["type"],
                table_data["ignore_geometry"],
            )
            for table_name, table_data in self.data["configuration"][
                "tables"
            ].items()
        }

    def get_tables(self):
        """
        Retorna o dicionário de tabelas.
        :return: Dicionário contendo informações das tabelas a serem processadas.
        """
        return self.tables
    def get_data_erros(df,column_types,name):
        try:
            df.astype(column_types)
        except ValueError:
            print('aqui')
    def get_numeric_erros(df,column_types,name): #TODO criar um log txt explicitando os erros
        list_error = []
        for column in df.columns:
            if column in column_types and column_types[column] != "category":
                for index,value in enumerate(df[column]):
                    try:
                        if "int" in column_types:
                            int(value)
                        else:
                            float(value)
                    except ValueError:
                        list_error.append(index)
                        print(f'Erro de preenchimento da BDGD localizado no elemento {name} de código {df.loc[index, "COD_ID"]} coluna {column}')     
    
    @staticmethod
    def convert_data_types(df, column_types, name): #TODO mostrar quais são os elementos com erro de preenchimento
        """
        Converte os tipos de dados das colunas do DataFrame fornecido.
        :param df: DataFrame a ser processado.
        :param column_types: Dicionário contendo mapeamento de colunas para tipos de dados.
        :return: DataFrame com tipos de dados convertidos.
        """
        try:
            return df.astype(column_types)
        except ValueError:
            JsonData.get_numeric_erros(df,column_types,name)
            return df
        
    def create_geodataframes(self, file_name, runs=1):
        """
        Cria GeoDataFrames a partir de um arquivo de entrada e coleta estatísticas.
        :param file_name: Nome do arquivo de entrada.
        :param runs: Número de vezes que cada tabela será carregada e convertida (padrão: 1).
        :return: Dicionário contendo GeoDataFrames e estatísticas.
        """
        geodataframes = {}

        for table_name, table in self.tables.items():
            load_times = []
            conversion_times = []
            gdf_converted = None

            for _ in range(runs):
                start_time = time.time()
                print(f'Creating geodataframe {table.name}')
                gdf_ = gpd.read_file(file_name, layer=table.name,
                                     columns=table.columns,ignore_geometry=table.ignore_geometry, 
                                     engine='pyogrio', use_arrow=True)  # ! ignore_geometry não funciona, pq este parâmetro espera um bool e está recebendo str
                start_conversion_time = time.time()
                gdf_converted = self.convert_data_types(gdf_, table.data_types, table.name)
                end_time = time.time()
                
                load_times.append(start_conversion_time - start_time)
                conversion_times.append(end_time - start_conversion_time)
            load_time_avg = sum(load_times) / len(load_times)
            conversion_time_avg = sum(conversion_times) / len(conversion_times)
            mem_usage = gdf_converted.memory_usage(index=True, deep=True).sum() / 1024 ** 2

            geodataframes[table_name] = {
                'gdf': gdf_converted,
                'memory_usage': mem_usage,
                'load_time_avg': load_time_avg,
                'conversion_time_avg': conversion_time_avg,
                'ignore_geometry': table.ignore_geometry
            }
        return geodataframes

    def create_geodataframes_lista_ctmt(self, file_name):
        """
        :return: Dicionário contendo GeoDataFrames.
        """
        geodataframes = {}

        for table_name, table in self.tables.items():
            gdf_ = gpd.read_file(file_name, layer="CTMT", columns=table.columns,
                                 engine='pyogrio', use_arrow=True)

            geodataframes[table_name] = {
                'gdf': gdf_
            }
        return geodataframes
    
    def scan_bdgd(self, file_name, output_file,lista_ctmt, runs=1):
        """
        Cria GeoDataFrames a partir de um arquivo de entrada e coleta estatísticas.
        :param file_name: Nome do arquivo de entrada.
        :param runs: Número de vezes que cada tabela será carregada e convertida (padrão: 1).
        :return: Dicionário contendo GeoDataFrames e estatísticas.
        """
        geodataframes = {}
        #path = os.path.join(output_file, "log_erros_dados.txt")
        lista_ctmt = []
        lista_segcon = []
        lista_untrmt = []
        lista_crvcrg = []

        path = os.path.join(output_file, 'erros_dados.csv')
        with open(path, 'w', newline='') as file:
            colunas = ['Tabela','Identificador','Código','Atributo','Valor']
            cabecalho = ','.join(colunas)
            file.write(cabecalho + '\n')
        #with open(path, "w") as file:
            for table_name, table in self.tables.items():
                gdf_converted = None
                print(f'Creating geodataframe {table.name}')
                gdf_ = gpd.read_file(file_name, layer=table.name,
                                        columns=table.columns, 
                                        engine='pyogrio', use_arrow=True)
                
                if table_name == 'CTMT':
                    lista_ctmt = gdf_["COD_ID"].tolist()
                if table_name == 'SEGCON':
                    lista_segcon = gdf_["COD_ID"].tolist()
                if table_name == 'CRVCRG':
                    lista_crvcrg = gdf_["COD_ID"].tolist()
                if table_name == 'UNTRMT':
                    lista_untrmt = gdf_["COD_ID"].tolist()

                for column in table.columns:
                    if type(table.data_types[column]) is list: 
                        for index,value in enumerate(gdf_[column]):
                            if pd.isnull(value) or value == "" or value == None:
                                file.write(f"{table_name}, {table.columns[0]},{gdf_.loc[index,table.columns[0]]},{column},{gdf_.loc[index,column]} \n")
                                continue
                            if value not in table.data_types[column]:
                                file.write(f"{table_name}, {table.columns[0]},{gdf_.loc[index,table.columns[0]]},{column},{gdf_.loc[index,column]} \n")
                    elif table.data_types[column] == 'int':
                        for index,value in enumerate(gdf_[column]):
                            try:
                                int(value)
                                continue
                            except ValueError or TypeError: 
                                file.write(f"{table_name}, {table.columns[0]},{gdf_.loc[index,table.columns[0]]},{column},{gdf_.loc[index,column]} \n")
                    elif table.data_types[column] == 'float':
                        for index,value in enumerate(gdf_[column]):
                            try:
                                float(value)
                                continue
                            except ValueError or TypeError: 
                                file.write(f"{table_name}, {table.columns[0]},{gdf_.loc[index,table.columns[0]]},{column},{gdf_.loc[index,column]} \n")
                    elif table.data_types[column] == 'string':
                        for index,value in enumerate(gdf_[column]):
                            try:
                                str(value)
                                continue
                            except ValueError or TypeError: 
                                file.write(f"{table_name}, {table.columns[0]},{gdf_.loc[index,table.columns[0]]},{column},{gdf_.loc[index,column]} \n")
                    elif table.data_types[column] == "category":
                        if column == 'CTMT':
                            for index,value in enumerate(gdf_[column]):
                                if value not in lista_ctmt:
                                    file.write(f"{table_name}, {table.columns[0]},{gdf_.loc[index,table.columns[0]]},{column},{gdf_.loc[index,column]} \n")
                        if column == 'TIP_CND':
                            for index,value in enumerate(gdf_[column]):
                                if value not in lista_segcon:
                                    file.write(f"{table_name}, {table.columns[0]},{gdf_.loc[index,table.columns[0]]},{column},{gdf_.loc[index,column]} \n")
                        if column == 'UNI_TR_MT':
                            for index,value in enumerate(gdf_[column]):
                                if value not in lista_untrmt:
                                    file.write(f"{table_name}, {table.columns[0]},{gdf_.loc[index,table.columns[0]]},{column},{gdf_.loc[index,column]} \n")
                        if column == 'TIP_CC':
                            for index,value in enumerate(gdf_[column]):
                                if value not in lista_crvcrg:
                                    file.write(f"{table_name}, {table.columns[0]},{gdf_.loc[index,table.columns[0]]},{column},{gdf_.loc[index,column]} \n")
                        else:
                            continue
                    else:
                        intervalo = eval(table.data_types[column])
                        inicio,fim = intervalo
                        lista = list(range(inicio,fim))
                        for index,value in enumerate(gdf_[column]):
                            if pd.isnull(value) or value == "" or value == None:
                                file.write(f"{table_name}, {table.columns[0]},{gdf_.loc[index,table.columns[0]]},{column},{gdf_.loc[index,column]} \n")
                                continue
                            if int(value) not in lista:
                                file.write(f"{table_name}, {table.columns[0]},{gdf_.loc[index,table.columns[0]]},{column},{gdf_.loc[index,column]} \n")
                                continue
