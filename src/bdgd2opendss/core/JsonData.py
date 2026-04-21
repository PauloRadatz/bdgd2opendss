import json
import time
import geopandas as gpd


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

    def sanitize_names(self, df, table_name=None):
        """
        Substitui hífens, pontos e espaços por underline em colunas de identificação e nós para evitar erros no OpenDSS.
        """
        cols_to_sanitize = ['COD_ID', 'PAC_1', 'PAC_2', 'PAC_3', 'PAC', 'UN_RE', 'UNI_TR_MT', 'UNI_TR_S', 'CEG_GD', 'PN_CON', 'RAMAL', 'TIP_CND', 'TIP_CC', 'PAC_INI', 'CTMT', 'SUB', 'CONJ']
        for col in cols_to_sanitize:
            if col in df.columns:
                mask = df[col].notna()
                if mask.any():
                    is_cat = df[col].dtype.name == 'category'
                    if is_cat:
                        df[col] = df[col].astype('object')
                    
                    df.loc[mask, col] = df.loc[mask, col].astype(str).str.replace(r'[\-\.\:\s]+', '_', regex=True)
                    
                    # Garantir que nomes de nós não sejam puramente numéricos (apenas para colunas de PAC)
                    if col in ['PAC_1', 'PAC_2', 'PAC_3', 'PAC']:
                        numeric_mask = df.loc[mask, col].str.match(r'^\d+$')
                        if numeric_mask.any():
                            df.loc[mask & (df[col].str.match(r'^\d+$')), col] = 'Node_' + df.loc[mask & (df[col].str.match(r'^\d+$')), col]
                    
                    if is_cat:
                        df[col] = df[col].astype('category')
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
            #TODO fazer um try/except aqui para, caso não exista a tabela, não quebrar o código
            for _ in range(runs):
                start_time = time.time()
                print(f'Creating geodataframe {table.name}')
                gdf_ = gpd.read_file(file_name, layer=table.name,
                                     columns=table.columns,ignore_geometry=table.ignore_geometry, 
                                     engine='pyogrio', use_arrow=True)  # ! ignore_geometry não funciona, pq este parâmetro espera um bool e está recebendo str
                start_conversion_time = time.time()
                gdf_converted = self.convert_data_types(gdf_, table.data_types, table.name)
                gdf_converted = self.sanitize_names(gdf_converted, table_name)
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
            gdf_ = self.sanitize_names(gdf_, table_name)

            geodataframes[table_name] = {
                'gdf': gdf_
            }
        return geodataframes

    def create_geodataframe_errors(self,file_name,runs=1):

        geodataframes = {}
        
        for table_name, table in self.tables.items():
            load_times = []
            conversion_times = []
            #TODO fazer um try/except aqui para, caso não exista a tabela, não quebrar o código
            for _ in range(runs):
                start_time = time.time()
                print(f'Creating geodataframe {table.name}')
                gdf_ = gpd.read_file(file_name, layer=table.name,
                                columns=table.columns,ignore_geometry=table.ignore_geometry, 
                                 engine='pyogrio', use_arrow=True)  # ! ignore_geometry não funciona, pq este parâmetro espera um bool e está recebendo str
                gdf_ = self.sanitize_names(gdf_, table_name)

            geodataframes[table_name] = gdf_
            
        return geodataframes,self.tables
    

        
