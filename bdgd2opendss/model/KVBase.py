from dataclasses import dataclass

# @dataclass
class KVBase:

    LV_kVbase = {}  # Dic
    MV_kVbase = []  # List

    def __init__(self):
        self.LV_kVbase = {}
        self.MV_kVbase = []

    # @staticmethod
    def create_voltage_bases(self,dicionario_kv):  # remover as tensões de secundário de fase aqui
        lista = []

        # TODO evitar tomar decisoes
        for value in dicionario_kv.values():

            if value >= 0.22:
                lista.append(value)
            else:
                ...

        x = set(lista)

        # TODO se empty ?
        if max(lista) == '0.38':

            try:
                x.remove('0.22')

            except KeyError:
                ...
        return (list(x))

    def get_kVbase_str(self):

        # cria lista de tensoes de base na baixa tensao
        LV_KVBase_lst = self.create_voltage_bases(self.LV_kVbase)

        # y.sort() # OLD CODE sort jah eh feito internamente no metodo

        # TODO which one should be first: MV(medium voltages) or LV (low voltage) ?
        # y.append(kv[0]) # OLD CODE
        # concat
        kVbase_lst = self.MV_kVbase + LV_KVBase_lst

        # string com voltages bases
        voltagebases = " ".join(str(z) for z in set(kVbase_lst))

        return voltagebases
