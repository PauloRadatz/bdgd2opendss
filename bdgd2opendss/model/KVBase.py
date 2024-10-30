from dataclasses import dataclass

@dataclass
class KVBase:

    # MV KVbase
    kv = []

    # LV KVbase
    dicionario_kv = {}

    def __init__(self):
        self.kv = {}
        self.dicionario_kv = {}

    def get_kVbase_str():

        # gets LV(low voltage) kVbases
        # lvLVbaseDic = self._transformer.getkVbaseDic()

        # cria lista de tensoes de base na baixa tensao
        lvLVbaseDic = create_voltage_bases(dicionario_kv)

        # y.sort() # OLD CODE sort jah eh feito internamente no metodo

        # gets MV(low voltage) kVbases
        kVbaseDic = kv

        # y.append(kv[0]) # OLD CODE

        # TODO which one should be first: MV(medium voltages) or LV (low voltage) ?

        # concat
        kVbaseDic += lvLVbaseDic

        # string com voltages bases
        voltagebases = " ".join(str(z) for z in set(kVbaseDic))

        return voltagebases

    @staticmethod
    def create_voltage_bases(dicionario_kv):  # remover as tensões de secundário de fase aqui
        lista = []

        # TODO evitar tomar decisoes
        for value in dicionario_kv.values():  # dicionario_kv.values() usar
            if value >= 0.22:
                lista.append(value)
            else:
                ...
        x = set(lista)
        if max(lista) == '0.38':
            try:
                x.remove('0.22')
            except KeyError:
                ...
        return (list(x))
