# -*- encoding: utf-8 -*-

from dataclasses import field
import pandas as pd
from bdgd2opendss import Circuit, LineCode, Line, LoadShape, Transformer, RegControl, Load, PVsystem
from bdgd2opendss.core import Utils
from bdgd2opendss.core.Utils import create_master_file, create_voltage_bases, create_dfs_coords, create_output_feeder_coords, inner_entities_tables
from bdgd2opendss.model.KVBase import KVBase
from bdgd2opendss.model import BusCoords
from bdgd2opendss.core.Settings import settings

class Case:

    _dfs: dict = field(init=False)

    @property
    def dfs(self):
        return self._dfs

    @property
    def jsonData(self):
        return self._jsonData

    # TODO estruturas nao utilizadas, por hora comentadas.
    '''
    _circuit: list[Circuit] = field(init=False)
    _line_codes: list[LineCode] = field(init=False)
    _lines_SSDBT: list[Line] = field(init=False)
    _lines_SSDMT: list[Line] = field(init=False)
    _lines_RAMLIG: list[Line] = field(init=False)
    _load_shapes: list[LoadShape] = field(init=False)
    _transformers: list[Transformer] = field(init=False)
    _regcontrols: list[RegControl] = field(init=False)
    _loads: list[Load] = field(init=False)
    _MVloads: list[Load] = field(init=False)
    _PVsystems: list[PVsystem] = field(init=False)
    '''

    def __init__(self, jsonData: dict, geodataframes: dict, folder_bdgd, feeder, output_folder):

        self._jsonData = jsonData
        self._dfs = geodataframes
        self.folder_bdgd = folder_bdgd
        self.feeder = feeder
        self.output_folder = output_folder

        print(f"\nFeeder: {feeder}")

        # init list
        self.list_files_name = []

        # this object will keep track of the kVBase
        self._kVbaseObj = KVBase()

        # reads BDGD gdb files and populates the object Case
        self.PopulaCase()

    ''' # TODO comentei codigo nao utilizado
    @dfs.setter
    def dfs(self, value: dict):
        self._dfs = value

    @property
    def circuitos(self):
        return self.#_circuit

    @circuitos.setter
    def circuitos(self, value):
        self.#_circuit = value

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

    def circuit_names(self):
        if self.#_circuit is not None:
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
    '''

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

        # TODO ERRO Unresolved reference 're' /Suponho q temos q adicionar 1 pacote de RegExp no requirements
        setattr(linecode_, f"_linecode_{i}", re.sub(pattern, repl, input_str))

    def output_master(self, file_names, tip_dia="", mes=""):

        master = "clear\n"

        # TODO 1. CONSERTAR !! O codigo dos IF e ELSE ESTAO IGUAIS !!!
        #  Qual eh a ideia ? Processar o redirect da "GD" diferente ?

        # TODO 2. Vejo como fragil esta solucao de atrelar algo (que nao sei o que eh) ao nome do arquivo, neste caso GD.
        #  NA verdade o if deveria verificar o parametro do usuario se ele vai gerar as GDs ou nao
        for i in file_names:
            if i[:2] == "GD":
                master = master + f'!Redirect "{i}"\n'
            else:
                master = master + f'Redirect "{i}"\n'

        # get voltage base string
        vBase_str = self._kVbaseObj.get_kVbase_str()

        master += f'''Set mode = daily
Set Voltagebases = [{vBase_str}]
Calc Voltagebases
Set tolerance = 0.0001
Set maxcontroliter = 10
!Set algorithm = newton
!Solve mode = direct
Solve
buscoords buscoords.csv'''

        create_master_file(file_name=f'Master_{tip_dia}_{mes}', feeder=self.feeder, master_content=master, output_folder=self.output_folder)

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

        # TODO de fato quebrou, gerando excetion: UnboundLocalError: local variable 'indice' referenced before assignment
        #  Correcao temporaria. Inicializei indice abaixo
        indice = 0

        for elemento in file_names:
            if "Cargas" in elemento:
                indice = file_names.index(elemento)

        # TODO Este tipo de indexacao eh fragil. Se a ordem dos appends muda, o codigo quebra.
        base_string_BT = file_names[indice - 2]
        base_string_MT = file_names[indice - 1]
        base_string_PIP = file_names[indice]

        for tip_dia in ['DU', 'SA', 'DO']:

            aux_BT = base_string_BT.replace('_DU', f'_{tip_dia}')
            aux_MT = base_string_MT.replace('_DU', f'_{tip_dia}')
            aux_PIP = base_string_PIP.replace('_DU', f'_{tip_dia}')

            for mes in meses:
                file_names[indice - 2] = aux_BT.replace('01_', f'{mes}_')
                file_names[indice - 1] = aux_MT.replace('01_', f'{mes}_')
                file_names[indice] = aux_PIP.replace('01_', f'{mes}_')

                self.output_master(tip_dia=tip_dia, mes=mes, file_names=file_names)

    # this method populates Case object with data from BDGD .gdb files
    def PopulaCase(self):

        self.GenGeographicCoord()

        self.Populates_CTMT()

        self.Populates_SEGCON()

        self.Populates_UNTRMT()

        self.Populates_Entity()

        # TODO erro na BDGD CPFL
        #  An error occurred: unsupported operand type(s) for /: 'float' and 'str'
        self.Populates_UNREMT()

        self.Popula_CRVCRG()

        self.Populates_UCMT()

        self.Populates_UCBT()

        self.Populates_PIP()

        self.Populates_UGBT()

        self.Populates_UGMT()

        # creates dss files
        self.output_master(self.list_files_name)
        self.create_outputs_masters(self.list_files_name)

    # generates the geographic coordinates
    def GenGeographicCoord(self):

        if settings.gerCoord:
            gdf_SSDMT, gdf_SSDBT = create_dfs_coords(self.folder_bdgd, self.feeder)
            #
            df_coords = BusCoords.get_buscoords(gdf_SSDMT, gdf_SSDBT)
            #
            Utils.create_output_feeder_coords(df_coords, feeder=self.feeder, output_folder=self.output_folder)

    # CTMT
    # TODO colocar o local e a pasta criada no create from json. // ja foi feito??
    def Populates_CTMT(self):

        alimentador = self.feeder

        try:

            self._kVbaseObj, fileName = Circuit.create_circuit_from_json(self.jsonData, self.dfs['CTMT']['gdf'].query("COD_ID==@alimentador"),
                                                                         self._kVbaseObj,
                                                                         pastadesaida=self.output_folder)

            # TODO lets try to move all these calling to one place
            self.list_files_name.append(fileName)

        except UnboundLocalError:
            print("Error in CTMT.\n")

    # SEGCON
    def Populates_SEGCON(self):

        try:

            line_codes_tmp, fileName = LineCode.create_linecode_from_json(self.jsonData, self.dfs['SEGCON']['gdf'],
                                                                           self.feeder, pastadesaida=self.output_folder)
            self.list_files_name.append(fileName)

        except UnboundLocalError:
            print("Error in SEGCON.\n")

    # UCBT
    def Popula_UCBT(self):

        alimentador = self.feeder

        if not self.dfs['UCBT_tab']['gdf'].query("CTMT == @alimentador").empty:

            try:
                loads_tmp, fileName = Load.create_load_from_json(self.jsonData,
                                                              self.dfs['UCBT_tab']['gdf'].query("CTMT==@alimentador"),
                                                              self.dfs['CRVCRG']['gdf'], 'UCBT_tab')

                self.list_files_name.append(fileName)

            except UnboundLocalError:
                print("Error in UCBT_tab\n")

        else:
            print(f'No UCBT found for this feeder.\n')

    # SSDMT
    def Populates_Entity(self):

        alimentador = self.feeder

        for entity in ['SSDMT', 'UNSEMT', 'SSDBT', 'UNSEBT', 'RAMLIG']:

            if not self.dfs[entity]['gdf'].query("CTMT == @alimentador").empty:

                try:
                    lines_tmp, fileName, aux_em = Line.create_line_from_json(self.jsonData, self.dfs[entity]['gdf'].query("CTMT==@alimentador"),
                                                                             entity, ramal_30m=settings.limitRamal30m, pastadesaida=self.output_folder)

                    # TODO we can concat fileName + aux_em inside create_line_from_json ans return a List[str]
                    self.list_files_name.append(fileName)

                    if aux_em != "":
                        self.list_files_name.append(aux_em)

                except UnboundLocalError:
                    print(f"Error in {entity}.\n")

    # UNREMT
    def Populates_UNREMT(self):

        alimentador = self.feeder

        # do the merge before checking if result set is empty
        merged_dfs = inner_entities_tables(self.dfs['EQRE']['gdf'],
                                           self.dfs['UNREMT']['gdf'].query("CTMT==@alimentador"),
                                           left_column='UN_RE', right_column='COD_ID')

        if not merged_dfs.query("CTMT == @alimentador").empty:

            try:
                regcontrols_tmp, fileName = RegControl.create_regcontrol_from_json(self.jsonData, merged_dfs, pastadesaida=self.output_folder)

                self.list_files_name.append(fileName)

            except UnboundLocalError:
                print("Error in UNREMT.\n")

        else:
            if self.dfs['UNREMT']['gdf'].query("CTMT == @alimentador").empty:
                print("No RegControls found for this feeder.\n")
            else:
                print("Error. Please, check the association EQRE/UNREMT for this feeder.\n")

    # EQTRMT
    def Populates_UNTRMT(self):

        alimentador = self.feeder

        merged_dfs = inner_entities_tables(self.dfs['EQTRMT']['gdf'],
                                           self.dfs['UNTRMT']['gdf'].query("CTMT==@alimentador"),
                                           left_column='UNI_TR_MT', right_column='COD_ID')
        if not merged_dfs.query("CTMT == @alimentador").empty:
            try:

                self._kVbaseObj, fileName = Transformer.create_transformer_from_json(self.jsonData, merged_dfs,
                                                                                     self._kVbaseObj, pastadesaida=self.output_folder)

                self.list_files_name.append(fileName)

            except UnboundLocalError:
                print("Error in UNTRMT.\n")

        else:
            print("Error. Please, check the association EQTRMT/UNTRMT for this feeder.\n")

    # CRVCRG
    def Popula_CRVCRG(self):

        try:
            load_shapes_tmp, fileName = LoadShape.create_loadshape_from_json(self.jsonData, self.dfs['CRVCRG']['gdf'],
                                                                             self.feeder, pastadesaida=self.output_folder)

            self.list_files_name.append(fileName)

        except UnboundLocalError:
            print("Error in CRVCRG\n")

    # UCBT_tab
    def Populates_UCBT(self):

        alimentador = self.feeder

        if not self.dfs['UCBT_tab']['gdf'].query("CTMT == @alimentador").empty:

            dfs = pd.DataFrame(self._dfs['UCBT_tab']['gdf'].query("CTMT == @alimentador"))

            df_ucbt = pd.DataFrame(dfs).groupby('COD_ID', as_index=False).agg({'PAC':'last','FAS_CON':'last','TEN_FORN':'last','TIP_CC':'last','UNI_TR_MT':'last',
                'CTMT':'last','RAMAL':'last','DAT_CON':'last','ENE_01':'sum','ENE_02':'sum','ENE_03':'sum','ENE_04':'sum','ENE_05':'sum',
                'ENE_06':'sum','ENE_07': 'sum','ENE_08': 'sum','ENE_09':'sum','ENE_10':'sum','ENE_11':'sum','ENE_12':'sum'})#criar um dicionário 'last'

            try:
                loads_tmp, fileName = Load.create_load_from_json(self.jsonData, df_ucbt, self.dfs['CRVCRG']['gdf'],
                                                                 'UCBT_tab', self._kVbaseObj,pastadesaida=self.output_folder)

                self.list_files_name.append(fileName)

            except UnboundLocalError:
                print("Error in UCBT_tab\n")

        else:
            print(f'No UCBT found for this feeder.\n')

    # PIP
    def Populates_PIP(self):

        alimentador = self.feeder

        if not self.dfs['PIP']['gdf'].query("CTMT == @alimentador").empty:

            try:
                loads_tmp, fileName = Load.create_load_from_json(self.jsonData,
                                                                  self.dfs['PIP']['gdf'].query("CTMT==@alimentador"),
                                                                  self.dfs['CRVCRG']['gdf'], 'PIP', self._kVbaseObj,
                                                                  pastadesaida=self.output_folder)
                self.list_files_name.append(fileName)

            except UnboundLocalError:
                print("Error in PIP\n")

        else:
            print(f'No PIP found for this feeder.\n')

    # UCMT_tab
    def Populates_UCMT(self):

        alimentador = self.feeder

        if not self.dfs['UCMT_tab']['gdf'].query("CTMT == @alimentador").empty:

            dfs = pd.DataFrame(self._dfs['UCMT_tab']['gdf'].query("CTMT == @alimentador"))

            # DEBUG print(dfs.columns)

            # TODO ?? criar um dicionário 'last'
            df_ucmt = pd.DataFrame(dfs).groupby('COD_ID', as_index=False).agg({'PAC':'last','FAS_CON':'last','TEN_FORN':'last','TIP_CC':'last',
                'CTMT':'last','PN_CON':'last','ENE_01':'sum','ENE_02':'sum','ENE_03':'sum','ENE_04':'sum','ENE_05':'sum',
                'ENE_06':'sum','ENE_07': 'sum','ENE_08': 'sum','ENE_09':'sum','ENE_10':'sum','ENE_11':'sum','ENE_12':'sum'})

            try:
                loads_tmp, fileName = Load.create_load_from_json(self.jsonData, df_ucmt, self.dfs['CRVCRG']['gdf'],
                                                                 'UCMT_tab', self._kVbaseObj, pastadesaida=self.output_folder)

                self.list_files_name.append(fileName)

            except UnboundLocalError:
                print("Error in UCMT\n")
        else:
            print(f'No UCMT found for this feeder.\n')

    # UGBT_tab
    def Populates_UGBT(self):

        alimentador = self.feeder

        if not self.dfs['UGBT_tab']['gdf'].query("CTMT == @alimentador").empty:

            try:
                pvsystems_tmp, fileName = PVsystem.create_pvsystem_from_json(self.jsonData,
                                                                             self.dfs['UGBT_tab']['gdf'].query("CTMT==@alimentador"),
                                                                             'UGBT_tab', self._kVbaseObj, pastadesaida=self.output_folder)
                self.list_files_name.append(fileName)

            except UnboundLocalError:
                print("Error in UGBT_tab\n")

        else:
            print("No UGBT found for this feeder. \n")

    # UGMT_tab
    def Populates_UGMT(self):

        alimentador = self.feeder

        if not self.dfs['UGMT_tab']['gdf'].query("CTMT == @alimentador").empty:

            try:
                pvsystems_tmp, fileName = PVsystem.create_pvsystem_from_json(self.jsonData,
                                                                              self.dfs['UGMT_tab']['gdf'].query("CTMT==@alimentador"),
                                                                             'UGMT_tab', self._kVbaseObj,
                                                                             pastadesaida=self.output_folder)
                self.list_files_name.append(fileName)

            except UnboundLocalError:
                print("Error in UGBT_tab\n")
        else:
            print("No UGMT found for this feeder. \n")
