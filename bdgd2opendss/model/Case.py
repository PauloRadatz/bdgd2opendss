# -*- encoding: utf-8 -*-

from dataclasses import dataclass, field

from bdgd2opendss import Circuit, LineCode, Line, LoadShape, Transformer, RegControl, Load, PVsystem
from bdgd2opendss.core.Utils import create_master_file, create_voltage_bases
from bdgd2opendss.model.Transformer import dicionario_kv, mtkv


@dataclass
class Case:
    _id: str = ""
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

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

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

    def line_name(self):
        return [l.line for l in self.lines]

    def load_shape_names(self):
        return [ls.load_shape for ls in self.load_shapes]

    def transformers_names(self):
        return [tr.transformer for tr in self.transformers]

    def regcontrols_names(self):
        return [rgc.regcontrol for rgc in self.regcontrols]

    def loads_names(self):
        return [ld.load for ld in self.loads]

    def pvsystems_names(self):
        return [pv.pvsystem for pv in self.PVsystems]

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

        x = set(mtkv)  #cria lista de tensões de base na média tensão
        y = create_voltage_bases(dicionario_kv) #cria lista de tensões de base na baixa tensão
        for k in list(x):
            y.append(k)
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

        create_master_file(file_name=f'Master_{tip_dia}_{mes}', feeder=self.id, master_content=master)

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

        for elemento in file_names:
            if "Cargas" in elemento:
                indice = file_names.index(elemento)

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
