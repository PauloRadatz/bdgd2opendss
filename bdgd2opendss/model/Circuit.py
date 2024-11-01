# -*- encoding: utf-8 -*-

# Não remover a linha de importação abaixo
from typing import Any, List
import geopandas as gpd

from tqdm import tqdm

from bdgd2opendss.model.Converter import convert_tten
from bdgd2opendss.model.KVBase import KVBase
from bdgd2opendss.core.Utils import create_output_file
from dataclasses import dataclass

# TODO finish the refactory
_kVbaseStr = []

@dataclass
class Circuit:

    _circuit: str = ""

    @property
    def circuit(self) -> str:
        return self._circuit

    @circuit.setter
    def circuit(self, value):
        self._circuit = f"Circuit.{value}"

    ''' # TODO comentei estruturas nao utilizadas
    _arquivo: str = ""
    _basekv: float = 0.0
    _pu: float = 0.0
    _bus1: str = ""
    _r1: float = 0.0000
    _x1: float = 0.0001

    @property
    def arquivo(self) -> str:
        return self._arquivo

    @arquivo.setter
    def arquivo(self, value):
        self._arquivo = value

    @property
    def pu(self) -> float:
        return self._pu

    @pu.setter
    def pu(self, value):
        self._pu = value

    @property
    def bus1(self) -> str:
        return self._bus1

    @bus1.setter
    def bus1(self, value):
        self._bus1 = value

    @property
    def r1(self) -> float:
        return self._r1

    @r1.setter
    def r1(self, value):
        self._r1 = value

    @property
    def x1(self) -> float:
        return self._x1

    @x1.setter
    def x1(self, value):
        self._x1 = value

    def full_string(self) -> str:
        return f"New \"Circuit.{self.circuit}\" basekv={self.basekv} pu={self.pu} " \
               f"bus1=\"{self.bus1}\" r1={self.r1} x1={self.x1}"

    def __repr__(self):
        return f"New \"Circuit.{self.circuit}\" basekv={self.basekv} pu={self.pu} " \
               f"bus1=\"{self.bus1}\" r1={self.r1} x1={self.x1}"
   '''

    @staticmethod
    def getKVBase_circuit_str():

        # if is empty informs user
        if not _kVbaseStr:
            print(f"\nError: {_kVbaseStr} is empty.")

        return(_kVbaseStr)
        #return(kv[0]) # OLD CODE

    @staticmethod
    def _process_static(circuit_, value):
        """Static method to process the static configuration for a Circuit object.

        Args:
            circuit_ (object): A Circuit object being updated.
            value (dict): A dictionary containing the static configuration.

        This method processes the static configuration by iterating through the
        key-value pairs of the 'value' dictionary and directly setting the
        corresponding attribute on the Circuit object with the static value.
        """
        for static_key, static_value in value.items():
            setattr(circuit_, f"_{static_key}", static_value)

    @staticmethod
    def _process_direct_mapping(circuit_, value, row):
        """Static method to process the direct mapping configuration for a Circuit object.

        Args:
            circuit_ (object): A Circuit object being updated.
            value (dict): A dictionary containing the direct mapping configuration.
            row (pd.Series): A row from the GeoDataFrame containing circuit-related data.

        This method processes the direct mapping configuration by iterating through the
        key-value pairs of the 'value' dictionary and directly setting the corresponding
        attribute on the Circuit object using the value from the row.
        """
        for mapping_key, mapping_value in value.items():
            setattr(circuit_, f"_{mapping_key}", row[mapping_value])

    @staticmethod
    def _process_indirect_mapping(circuit_, value, row):
        """Static method to process the indirect mapping configuration for a Circuit object.

        Args:
            circuit_ (object): A Circuit object being updated.
            value (dict): A dictionary containing the indirect mapping configuration.
            row (pd.Series): A row from the GeoDataFrame containing circuit-related data.

        This method processes the indirect mapping configuration by iterating through the
        key-value pairs of the 'value' dictionary. If the value is a list, it treats the
        first element as a parameter name and the second element as a function name. The
        method then retrieves the parameter value from the row and calls the specified
        function with that parameter value. The result is then set as an attribute on the
        Circuit object.

        If the value is not a list, the method directly sets the corresponding attribute on
        the Circuit object using the value from the row.
        """
        for mapping_key, mapping_value in value.items():
            if isinstance(mapping_value, list):
                param_name, function_name = mapping_value
                function_ = globals()[function_name]
                param_value = row[param_name]
                setattr(circuit_, f"_{mapping_key}", function_(str(param_value)))        # corrigingo para string para encontrar valor no dicionario
                if mapping_key == 'basekv':
                    _kVbaseStr.append(function_(str(param_value)))
            else:
                setattr(circuit_, f"_{mapping_key}", row[mapping_value])

    @classmethod
    def create_circuit_from_json(cls, json_data: Any, dataframe: gpd.geodataframe.GeoDataFrame, _kVbaseObj: KVBase, pastadesaida:str = "") -> List:
        """Class method to create a list of Circuit objects from JSON data and a GeoDataFrame.

        Args:
            cls: The class for which this method is called.
            json_data (Any): JSON data containing circuit configuration information.
            dataframe (gpd.geodataframe.GeoDataFrame): A GeoDataFrame containing circuit-related data.

        Returns:
            List[cls]:

        This method iterates through the rows of the given GeoDataFrame, processes the JSON data,
        and creates Circuit objects accordingly. It updates the progress bar description with the
        current circuit number being processed.

        The JSON data must have the following structure:
            {
                "elements": {
                    "Circuit": {
                        "CTMT": {
                            "direct_mapping": ...,
                            "indirect_mapping": ...,
                            "static": ...
                        }
                    }
                }
            }

        The keys "direct_mapping", "indirect_mapping", and "static" are used to determine
        how to process each row in the GeoDataFrame and update the Circuit objects accordingly.
        """

        circuits = []
        circuit_config = json_data['elements']['Circuit']['CTMT']

        progress_bar = tqdm(dataframe.iterrows(), total=len(dataframe), desc="Circuit", unit=" circuits", ncols=100)
        for _, row in progress_bar:
            circuit_ = cls()

            for key, value in circuit_config.items():
                if key == "direct_mapping":
                    cls._process_direct_mapping(circuit_, value, row)
                elif key == "indirect_mapping":
                    cls._process_indirect_mapping(circuit_, value, row)
                elif key == "static":
                    cls._process_static(circuit_, value)
            circuits.append(circuit_)
            progress_bar.set_description(f"Processing Circuit {_+1}")

        file_name = create_output_file(circuits, circuit_config["arquivo"], output_folder=pastadesaida,feeder=circuit_.circuit)

        # TODO solucao temporaria
        _kVbaseObj._kVbaseStr = _kVbaseStr

        # OBS: changed signature
        return _kVbaseObj, file_name
