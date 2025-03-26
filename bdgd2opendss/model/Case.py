# -*- encoding: utf-8 -*-

from dataclasses import dataclass, field
import pandas as pd

from bdgd2opendss import Circuit, LineCode, Line, LoadShape, Transformer, RegControl, Load, PVsystem
from bdgd2opendss.core.Utils import create_master_file, create_voltage_bases, get_cod_year_bdgd, create_df_trafos_vazios,get_configuration
from bdgd2opendss.model.Count_days import count_day_type
from bdgd2opendss.model import BusCoords
from bdgd2opendss.core.Settings import settings
from bdgd2opendss.core import Utils
from bdgd2opendss.model.EnergyMeters import create_energymeters
#from bdgd2opendss.model.KVBase import KVBase

@dataclass
class Case:
    #_id: str = "" # OLD CODE alterado p/ feeder
    _circuitos: list[Circuit] = field(init=False)
    _line_codes: list[LineCode] = field(init=False)
    _lines_SSDBT: list[Line] = field(init=False)
    _lines_SSDMT: list[Line] = field(init=False)
    _lines_RAMLIG: list[Line] = field(init=False)
    _load_shapes: list[LoadShape] = field(init=False)
    _transformers: list[Transformer] = field(init=False)
    _regcontrols: list[RegControl] = field(init=False)
    _loads: list[Load] = field(init=False)
    _PVsystems: list[PVsystem] = field(init=False)
    _dfs: dict = field(init=False)
    
    def __init__(self, jsonData, geodataframes, folder_bdgd, feeder, output_folder):
        self._jsonData = jsonData
        self._dfs = geodataframes
        self.folder_bdgd = folder_bdgd
        self.feeder = feeder
        self.output_folder = output_folder

        if settings.TipoBDGD: #BDGD privada
            self.ucbt = "UCBT"
            self.ucmt = "UCMT"
            self.ugbt = "UGBT"
            self.ugmt = "UGMT"
        else: #BDGD pública
            self.ucbt = "UCBT_tab"
            self.ucmt = "UCMT_tab"
            self.ugbt = "UGBT_tab"
            self.ugmt = "UGMT_tab"
        
        print(f"\nFeeder: {feeder}")

        # init list
        self.list_files_name = []

    @property
    def circuitos(self):
        return self._circuitos

    @circuitos.setter
    def circuitos(self, value):
        self._circuitos = value

    @property
    def line_codes(self):
        return self._line_codes

    @line_codes.setter
    def line_codes(self, value):
        self._line_codes = value

    @property
    def lines_SSDBT(self):
        return self._lines_SSDBT

    @lines_SSDBT.setter
    def lines_SSDBT(self, value):
        self._lines_SSDBT = value

    @property
    def load_shapes(self):
        return self._load_shapes

    @load_shapes.setter
    def load_shapes(self, value):
        self._load_shapes = value

    @property
    def transformers(self):
        return self._transformers

    @transformers.setter
    def transformers(self, value):
        self._transformers = value

    @property
    def regcontrols(self):
        return self._regcontrols

    @regcontrols.setter
    def regcontrols(self, value):
        self._regcontrols = value

    @property
    def loads(self):
        return self._loads

    @loads.setter
    def loads(self, value):
        self._loads = value

    @property
    def pvsystems(self):  # mozart
        return self._PVsystems

    @pvsystems.setter
    def pvsystems(self, value):  # mozart
        self._PVsystems = value

    @property
    def dfs(self):
        return self._dfs

    @dfs.setter
    def dfs(self, value: dict):
        self._dfs = value

    def circuit_names(self):
        if self._circuitos is not None:
            return [c.circuit for c in self.circuitos]

    def line_code_names(self):
        return [l.linecode for l in self.line_codes]

    # TODO Warning Unresolved attribute reference 'lines' for class 'Case'
    def line_name(self):
        return [l.line for l in self.lines]

    # TODO Warning Unresolved attribute reference 'load_shape' for class 'LoadShape'
    def load_shape_names(self):
        return [ls.load_shape for ls in self.load_shapes]

    def transformers_names(self):
        return [tr.transformer for tr in self.transformers]

    # TODO Warning Unresolved attribute reference 'regcontrol' for class 'RegControl'
    def regcontrols_names(self):
        return [rgc.regcontrol for rgc in self.regcontrols]

    def loads_names(self):
        return [ld.load for ld in self.loads]

    # TODO Warning Unresolved attribute reference 'PVsystems' for class 'Case'
    def pvsystems_names(self):
        return [pv.pvsystem for pv in self.pvsystems]

    # TODO Warning Expected to return 'str', got no return
    def rename_linecode_string(linecode_, i, input_str: str) -> str:
        """
        This function re-writes the string identfying key places by specified parameters and insering caracteres.

        Args:

        Returns:

        In this case, it should modify the names of line, bus1, bus2 and linecode.

        """
        pattern = r'New "Linecode.(\d+)" nphases=(\d+)'

        def repl(match):
            linecode_num = match.group(1)
            nphases_value = match.group(2)
            return f'New "Linecode.{linecode_num}_{nphases_value}" nphases={nphases_value}'

        setattr(linecode_, f"_linecode_{i}", re.sub(pattern, repl, input_str))

    def output_master(self, file_names, tip_dia="", mes=""):

        master = "clear\n"
        y = create_voltage_bases(Transformer.dict_kv()) #cria lista de tensões de base na baixa tensão
        y.sort()
        #  TODO do jeito que esta, a variavel kv (declarada na classe circuit) entra neste metodo como 1 variavel global
        #   A mesma questao ocorre com a variavel dicionario_kv idealmente devemos refatorar e acessar estas variaveis por metodos jah
        #   que esta classe ja possui as variaveis _circuitos e _transformers (idealmente os metodos podem chegar o preecnhimento da variavel e
        #   qualquer dependencia (temporal) de se executar o circuit e transformer antes). Eg _circuitos.GetKv()
        y.append(Circuit.kvbase())
        voltagebases = " ".join(str(z) for z in set(y))
        for i in file_names:
            if i[:2] == "GD":
                master = master + f'!Redirect "{i}"\n'
            else:
                master = master + f'Redirect "{i}"\n'
        master = master + f'''Set mode = daily
Set Voltagebases = [{voltagebases}]
Calc Voltagebases
Set tolerance = 0.0001
Set maxcontroliter = 10
!Set algorithm = newton
!Solve mode = direct
Solve
buscoords buscoords.csv'''

        create_master_file(file_name=f'Master_{tip_dia}{mes}', feeder=self.feeder, master_content=master, output_folder=self.output_folder)

    def create_outputs_masters(self, file_names):
        """
        Creates dss_models_output masters based on file names.

        Args:
        - file_names (list): List of file names.

        Logic:
        - Generates a list of two-digit month numbers from 01 to 12.
        - Loops through each element in file_names.
        - Finds the index of the element containing "Cargas".
        - Uses the previous and current elements as base strings for modification.
        - Iterates through 'DU', 'SA', 'DO' and months to create modified file names.
        - Calls the 'output_master' method for each combination of tip_dia and month.

        Returns: None
        """
        meses = [f"{mes:02d}" for mes in range(1, 13)]

        indicebt = 0
        indicemt = 0

        for elemento in file_names:
            if "CargasBT" in elemento:
                indicebt = file_names.index(elemento)
            if "CargasMT" in elemento:
                indicemt = file_names.index(elemento)

        base_string_BT = file_names[indicebt]
        base_string_MT = file_names[indicemt]

        for tip_dia in ['DU', 'SA', 'DO']:

            aux_BT = base_string_BT.replace('_DU', f'_{tip_dia}')
            aux_MT = base_string_MT.replace('_DU', f'_{tip_dia}')


            for mes in meses:
                file_names[indicebt] = aux_BT.replace('01_', f'{mes}_',1)
                file_names[indicemt] = aux_MT.replace('01_', f'{mes}_',1)

                self.output_master(tip_dia=tip_dia, mes=mes, file_names=file_names)

    # this method populates Case object with data from BDGD
    def PopulaCase(self):
        get_cod_year_bdgd(self.folder_bdgd) #Extrai o código e o ano da BDGD para nomear os arquivos dss
        count_day_type(int(get_cod_year_bdgd()[0:4]))#calcula du,sa, do/feriados a partir do ano da BDGD
        get_configuration(feeder=self.feeder,output_folder=self.output_folder) #Identifica as configurações escolhidas pelo usuário e transforma em uma string

        self.GenGeographicCoord()

        self.Populates_CTMT()

        df_tramo, df_aux_trafo = Utils.create_aux_tramo(self.dfs,self.feeder)
        Utils.ordem_pacs(df_aux_tramo=df_tramo,pac_ctmt=Circuit.pac_ctmt()) #Define a ordem dos buses de acordo com o que a distribuidora usa
        Utils.elem_isolados(self.dfs,self.feeder,pac_ctmt=Circuit.pac_ctmt(),output_folder=self.output_folder) #Define quais são os elementos isolados e cria um log de elementos isolados
        Utils.seq_eletrica(self.dfs,self.feeder,pac=Circuit.pac_ctmt(),kvbase=Circuit.kvbase()) #Define as tensões no circuito com base nos transformadores

        self.Populates_SEGCON()

        self.Populates_UNTRMT()
        
        self.Populates_Entity()

        self.Populates_UNREMT()
        
        self.Populates_energymeters(df_tramo,df_aux_trafo)

        self.Popula_CRVCRG()

        self.Populates_UCBT()

        self.Populates_PIP()

        self.Populates_UCMT()
        #Load.export_df_loads()#exporta tabela de perdas técnicas para cargas
        self.Populates_UGBT()

        self.Populates_UGMT()

        # creates dss files
        self.output_master(self.list_files_name)
        self.create_outputs_masters(self.list_files_name)

    # generates the geographic coordinates
    def GenGeographicCoord(self):

        if settings.gerCoord:
            #
            gdf_SSDMT, gdf_SSDBT = Utils.create_dfs_coords(self.folder_bdgd, self.feeder)
            #
            df_coords = BusCoords.get_buscoords(gdf_SSDMT, gdf_SSDBT)
            if df_coords is None:
                return("There's no coordinates for this feeder")
            #
            Utils.create_output_feeder_coords(df_coords, feeder=self.feeder, output_folder=self.output_folder)

    # CTMT
    def Populates_CTMT(self):#TODO colocar o local e a pasta criada no create from json

        alimentador = self.feeder

        try:
            circuitos, fileName = Circuit.create_circuit_from_json(self._jsonData, self._dfs['CTMT']['gdf'].query(
                "COD_ID==@alimentador"), pastadesaida=self.output_folder)
            self.list_files_name.append(fileName)

        except UnboundLocalError:
            print("Error in CTMT.\n")

    # SEGCON
    def Populates_SEGCON(self):

        try:
            self.line_codes, fileName = LineCode.create_linecode_from_json(self._jsonData, self.dfs['SEGCON']['gdf'],
                                                                           self.feeder, pastadesaida=self.output_folder)
            self.list_files_name.append(fileName)

        except UnboundLocalError:
            print("Error in SEGCON.\n")

    # UCBT
    def Popula_UCBT(self):

        if not self._dfs[self.ucbt]['gdf'].query("CTMT == @alimentador").empty:
            
            try:
                _loads, fileName = Load.create_load_from_json(self._jsonData,
                                                              self._dfs[self.ucbt]['gdf'].query("CTMT==@alimentador"),
                                                              self._dfs['CRVCRG']['gdf'], self.ucbt)
                self.list_files_name.append(fileName)

            except UnboundLocalError:
                print("Error in UCBT\n")

        else:
            print(f'No UCBT found for this feeder.\n')

    # SSDMT
    def Populates_Entity(self):

        alimentador = self.feeder

        for entity in ['SSDMT', 'UNSEMT', 'SSDBT', 'UNSEBT', 'RAMLIG']:
            if not self.dfs[entity]['gdf'].query("CTMT == @alimentador").empty:

                try:
                    self._lines_SSDMT, fileName = Line.create_line_from_json(self._jsonData,
                                                                                    self.dfs[entity]['gdf'].query(
                                                                                        "CTMT==@alimentador"),
                                                                                    entity, pastadesaida=self.output_folder)

                    self.list_files_name.append(fileName)

                except UnboundLocalError:
                    print(f"Error in {entity}.\n")

    # UNREMT
    def Populates_UNREMT(self):

        alimentador = self.feeder

        # do the merge before checking if result set is empty
        merged_dfs = Utils.inner_entities_tables(self.dfs['EQRE']['gdf'],
                                           self.dfs['UNREMT']['gdf'].query("CTMT==@alimentador"),
                                           left_column='UN_RE', right_column='COD_ID')
        
        Utils.adapt_regulators_names(merged_dfs,'regulator')
        if not merged_dfs.query("CTMT == @alimentador").empty:

            try:
                self.regcontrols, fileName = RegControl.create_regcontrol_from_json(self._jsonData, merged_dfs,pastadesaida=self.output_folder)
                self.list_files_name.append(fileName)

            except UnboundLocalError:
                print("Error in UNREMT.\n")
        else:
            print("No RegControls found for this feeder.\n")

    # EQTRMT
    def Populates_UNTRMT(self):
        alimentador = self.feeder
        merged_dfs = Utils.inner_entities_tables(self.dfs['EQTRMT']['gdf'],
                                           self.dfs['UNTRMT']['gdf'].query("CTMT==@alimentador"),
                                           left_column='UNI_TR_MT', right_column='COD_ID')
        
        Utils.adapt_regulators_names(merged_dfs,'transformer')
        #settings - criação de dataframe para eliminar transformadores em vazio
        if not self.dfs[self.ucbt]['gdf'].query("CTMT == @alimentador").empty and settings.intAdequarTrafoVazio:
            df_uc = pd.DataFrame(self._dfs[self.ucbt]['gdf'].query("CTMT == @alimentador"))
            df_ip = pd.DataFrame(self.dfs['PIP']['gdf'].query("CTMT==@alimentador"))
            Utils.create_df_trafos_vazios(df_uc,df_ip,merged_dfs)
        if not merged_dfs.query("CTMT == @alimentador").empty:
            try:
                self.transformers, fileName = Transformer.create_transformer_from_json(self._jsonData, merged_dfs, pastadesaida=self.output_folder)
                self.list_files_name.append(fileName)

            except UnboundLocalError:
                print("Error in UNTRMT.\n")

        else:
            print('No Transformers found for this feeder. \n')

    # CRVCRG
    def Popula_CRVCRG(self):

        try:
            _load_shapes, fileName = LoadShape.create_loadshape_from_json(self._jsonData, self._dfs['CRVCRG']['gdf'], self.feeder, pastadesaida=self.output_folder)
            self.list_files_name.append(fileName)
        except UnboundLocalError:
            print("Error in CRVCRG\n")

    # UCBT
    def Populates_UCBT(self):

        alimentador = self.feeder

        if not self.dfs[self.ucbt]['gdf'].query("CTMT == @alimentador").empty:
            dfs = pd.DataFrame(self._dfs[self.ucbt]['gdf'].query("CTMT == @alimentador"))
            df_ucbt = pd.DataFrame(dfs).groupby('COD_ID', as_index=False).agg({'PAC':'last','FAS_CON':'last','TEN_FORN':'last','TIP_CC':'last','UNI_TR_MT':'last',
                'CTMT':'last','RAMAL':'last','DAT_CON':'last','ENE_01':'sum','ENE_02':'sum','ENE_03':'sum','ENE_04':'sum','ENE_05':'sum',
                'ENE_06':'sum','ENE_07': 'sum','ENE_08': 'sum','ENE_09':'sum','ENE_10':'sum','ENE_11':'sum','ENE_12':'sum'})#criar um dicionário 'last'
            Utils.check_duplicate_loads_names(df_ucbt,"BT") #deve-se passar o tipo de consumidor (BT ou MT)
            
            try:
                self.loads, fileName = Load.create_load_from_json(self._jsonData,
                                                                  df_ucbt,
                                                                  self.dfs['CRVCRG']['gdf'], self.ucbt,pastadesaida=self.output_folder)
                self.list_files_name.append(fileName)

            except UnboundLocalError:
                print("Error in UCBT\n")

        else:
            print(f'No UCBT found for this feeder.\n')

    # PIP
    def Populates_PIP(self):

        alimentador = self.feeder

        if not self.dfs['PIP']['gdf'].query("CTMT == @alimentador").empty:

            try:
                self.loads, fileName = Load.create_load_from_json(self._jsonData,
                                                                  self.dfs['PIP']['gdf'].query("CTMT==@alimentador"),
                                                                  self.dfs['CRVCRG']['gdf'], 'PIP',pastadesaida=self.output_folder)
                #self.list_files_name.append(fileName) #já está sendo criado dentro do arquivo cargasBT 

            except UnboundLocalError:
                print("Error in PIP\n")

        else:
            print(f'No PIP found for this feeder.\n')

    # UCMT
    def Populates_UCMT(self):

        alimentador = self.feeder
        if not self.dfs[self.ucmt]['gdf'].query("CTMT == @alimentador").empty:
            dfs = pd.DataFrame(self._dfs[self.ucmt]['gdf'].query("CTMT == @alimentador"))
            df_ucmt = pd.DataFrame(dfs).groupby('COD_ID', as_index=False).agg({'PAC': 'last', 'FAS_CON':'last','TEN_FORN':'last','TIP_CC':'last',
                'CTMT':'last','PN_CON':'last','ENE_01':'sum','ENE_02':'sum','ENE_03':'sum','ENE_04':'sum','ENE_05':'sum',
                'ENE_06':'sum','ENE_07': 'sum','ENE_08': 'sum','ENE_09':'sum','ENE_10':'sum','ENE_11':'sum','ENE_12':'sum'})#criar um dicionário 'last'
            Utils.check_duplicate_loads_names(df_ucmt,"MT") #deve-se passar o tipo de consumidor (BT ou MT)
            try:
                self.loads, fileName = Load.create_load_from_json(self._jsonData,
                                                                  df_ucmt,
                                                                  self.dfs['CRVCRG']['gdf'], self.ucmt,pastadesaida=self.output_folder)
                self.list_files_name.append(fileName)

            except UnboundLocalError:
                print("Error in UCMT\n")
        else:
            print(f'No UCMT found for this feeder.\n')

    # UGBT
    def Populates_UGBT(self):

        alimentador = self.feeder

        if not self.dfs[self.ugbt]['gdf'].query("CTMT == @alimentador").empty:

            try:
                self.pvsystems, fileName = PVsystem.create_pvsystem_from_json(self._jsonData,
                                                                              self.dfs[self.ugbt]['gdf'].query(
                                                                                  "CTMT==@alimentador"), self.ugbt, pastadesaida=self.output_folder)
                self.list_files_name.append(fileName)
                
            except UnboundLocalError:
                print("Error in UGBT\n")

        else:
            print("No UGBT found for this feeder. \n")

    # UGMT
    def Populates_UGMT(self):

        alimentador = self.feeder

        if not self.dfs[self.ugmt]['gdf'].query("CTMT == @alimentador").empty:

            try:
                self.pvsystems, fileName = PVsystem.create_pvsystem_from_json(self._jsonData,
                                                                              self.dfs[self.ugmt]['gdf'].query(
                                                                                  "CTMT==@alimentador"), self.ugmt, pastadesaida=self.output_folder)
                self.list_files_name.append(fileName)

            except UnboundLocalError:
                print("Error in UGBT\n")
        else:
            print("No UGMT found for this feeder. \n")

    def Populates_energymeters(self,df_aux_tramo,df_aux_trafo):
        alimentador = self.feeder
        fileName = create_energymeters(df_aux_tramo,df_aux_trafo,self.feeder,self.output_folder)
        self.list_files_name.append(fileName)
