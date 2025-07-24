# -*- encoding: utf-8 -*-


def convert_tten(case):
    switch_dict = {
        "0": 0.0,
        "1": 0.11,
        "2": 0.115,
        "3": 0.120,
        "4": 0.121,
        "5": 0.125,
        "6": 0.127,
        "7": 0.208,
        "8": 0.216,
        "9": 0.2165,
        "10": 0.220,
        "11": 0.230,
        "12": 0.231,
        "13": 0.240,
        "14": 0.254,
        "15": 0.380,
        "16": 0.400,
        "17": 0.440,
        "18": 0.480,
        "19": 0.500,
        "20": 0.600,
        "21": 0.750,
        "22": 1.0,
        "23": 2.3,
        "24": 3.2,
        "25": 3.6,
        "26": 3.785,
        "27": 3.8,
        "28": 3.848,
        "29": 3.985,
        "30": 4.160,
        "31": 4.2,
        "32": 4.207,
        "33": 4.368,
        "34": 4.560,
        "35": 5,
        "36": 6,
        "37": 6.6,
        "38": 6.93,
        "39": 7.96,
        "40": 8.67,
        "103": 11,
        "41": 11.4,
        "104": 11.5,
        "42": 11.9,
        "43": 12.0,
        "44": 12.6,
        "45": 12.7,
        "105": 13,
        "46": 13.2,
        "47": 13.337,
        "48": 13.530,
        "49": 13.8,
        "50": 13.86,
        "51": 14.14,
        "52": 14.19,
        "53": 14.4,
        "54": 14.835,
        "55": 15,
        "56": 15.2,
        "57": 19.053,
        "58": 19.919,
        "106": 20,
        "59": 21,
        "60": 21.5,
        "61": 22,
        "62": 23,
        "63": 23.1,
        "64": 23.827,
        "65": 24,
        "66": 24.2,
        "67": 25,
        "68": 25.8,
        "69": 27,
        "70": 30,
        "71": 33,
        "72": 34.5,
        "73": 36,
        "74": 38,
        "75": 40,
        "76": 44,
        "77": 45,
        "78": 45.4,
        "79": 48,
        "80": 60,
        "81": 66,
        "107": 68,
        "82": 69,
        "83": 72.5,
        "108": 85,
        "84": 88,
        "85": 88.2,
        "86": 92,
        "87": 100,
        "88": 120,
        "89": 121,
        "90": 123,
        "91": 131.6,
        "92": 131.630,
        "93": 131.635,
        "94": 138,
        "95": 145,
        "96": 230,
        "97": 345,
        "109": 440,
        "98": 500,
        "99": 750,
        "100": 1000,
        "101": 245,
        "102": 550
    }
    return switch_dict.get(case, 'Invalid case')


def process_loadshape2(loadshape_list):
    """
        Process a list of 96 floating point numbers and return a list of 24
        floating point numbers. The function computes the mean of every four
        numbers in the input list and normalizes the resulting list between 0
        and 1.

        Parameters
        ----------
        loadshape_list : list of float
            The input list containing 96 floating point numbers.

        Returns
        -------
        list of float
            A list containing 24 floating point numbers, which are the mean of
            every four numbers in the input list, normalized between 0 and 1.

        """
    medias = [sum(loadshape_list[i:i + 4]) / 4 for i in range(0, len(loadshape_list), 4)]


    # Calculate the minimum and maximum values in the array
    max_value = max(medias)
    if max_value == 0:
        return [0]*24, medias
    return [media/max_value for media in medias], medias  # Set all values to 0.5 (midpoint)





def process_loadshape(loadshape_list):
    """
        Process a list of 96 floating point numbers and return a list of 24
        floating point numbers. The function computes the mean of every four
        numbers in the input list and normalizes the resulting list between 0
        and 1.

        Parameters
        ----------
        loadshape_list : list of float
            The input list containing 96 floating point numbers.

        Returns
        -------
        list of float
            A list containing 24 floating point numbers, which are the mean of
            every four numbers in the input list, normalized between 0 and 1.

        """

    medias = [sum(loadshape_list[i:i + 4]) / 4 for i in range(0, len(loadshape_list), 4)]



    # Calculate the minimum and maximum values in the array
    min_value = min(medias)
    max_value = max(medias)

    # Check if the range is zero
    if max_value - min_value == 0:
        # Handle the case when the range is zero (all values are the same)
        return [1 for _ in medias], medias  # Set all values to 0.5 (midpoint)

    else:
        # Normalize the array to the range [0, 1]
        return [(media - min_value) / (max_value - min_value) for media in medias], medias



def convert_tfascon_bus(case):
    switch_dict = {
        'ABCN': '1.2.3.4',
        'ABC': '1.2.3',
        'ABN': '1.2.4',
        'BCN': '2.3.4',
        'CAN': '3.1.4',
        'AX': '1.4.0',
        'BX': '2.4.0',
        'CX': '3.4.0',
        'AB': '1.2',
        'BC': '2.3',
        'CA': '3.1',
        'AN': '1.4',
        'BN': '2.4',
        'CN': '3.4',
        'A': '1',
        'B': '2',
        'C': '3',
        'N': '4',
    }
    return switch_dict.get(case, 'Invalid case')

def convert_tfascon_bus_prim(case):
    switch_dict = {
        'A': '1',
        'B': '2',
        'C': '3',
        'AN': '1.0',
        'BN': '2.0',
        'CN': '3.0',
        'AB': '1.2',
        'BC': '2.3',
        'CA': '3.1',
        'ABN': '1.2.0',
        'BCN': '2.3.0',
        'CAN': '3.1.0',
        'ABC': '1.2.3',
        'ABCN': '1.2.3.0',
    }
    return switch_dict.get(case, 'Invalid case')

def convert_tfascon_bus_sec(case):
    switch_dict = {
        'A': '1',
        'B': '2',
        'C': '3',
        'AN': '1.4',
        'BN': '2.4',
        'CN': '3.4',
        'AB': '1.2',
        'BC': '2.3',
        'CA': '3.1',
        'ABN': '1.2.4',
        'BCN': '2.3.4',
        'CAN': '3.1.4',
        'ABC': '1.2.3',
        'ABCN': '1.2.3.4',
    }
    return switch_dict.get(case, 'Invalid case')

def convert_tfascon_bus_terc(case):
    switch_dict = {
        'AN': '4.1',
        'BN': '4.2',
        'CN': '4.3',
        '0': 'XX'
    }
    return switch_dict.get(case, 'Invalid case')

def convert_tfascon_phases(case):
    switch_dict = {
        'ABCN': 3,
        'ABC': 3,
        'ABN': 2,
        'BCN': 2,
        'CAN': 2,
        'AX': 1,
        'BX': 1,
        'CX': 1,
        'AB': 2,
        'BC': 2,
        'CA': 2,
        'AN': 1,
        'BN': 1,
        'CN': 1,
        'A': 1,
        'B': 1,
        'C': 1,
        'N': 0,
    }
    return switch_dict.get(case, 'Invalid case')

def convert_tfascon_phases_trafo(case):
    switch_dict = {
        "A": "1",
        "B": "1",
        "C": "1",
        "AN": "1",
        "BN": "1",
        "CN": "1",
        "AB": "1",
        "BC": "1",
        "CA": "1",
        "ABN": "1",
        "BCN": "1",
        "CAN": "1",
        "ABC": "3",
        "ABCN": "3"
    }
    return switch_dict.get(case, 'Invalid case')

def convert_tfascon_phases_load(case):
    switch_dict = {
        "A": "1",
        "B": "1",
        "C": "1",
        "AN": "1",
        "BN": "1",
        "CN": "1",
        "AB": "1",
        "BC": "1",
        "CA": "1",
        "ABN": "1",
        "BCN": "1",
        "CAN": "1",
        "ABC": "3",
        "ABCN": "3"
    }
    return switch_dict.get(case, 'Invalid case')

def convert_tfascon_quant_fios(case):
    switch_dict = {
        'ABCN': 4,
        'ABC': 3,
        'ABN': 3,
        'BCN': 3,
        'CAN': 3,
        'AX': 3,
        'BX': 3,
        'CX': 3,
        'AB': 2,
        'BC': 2,
        'CA': 2,
        'AN': 2,
        'BN': 2,
        'CN': 2,
        'A': 1,
        'B': 1,
        'C': 1,
        'N': 1,
    }
    return switch_dict.get(case, 'Invalid case')

def convert_tfascon_conn_load(case):
    switch_dict = {
        "A": "Wye",
        "B": "Wye",
        "C": "Wye",
        "AN": "Wye",
        "BN": "Wye",
        "CN": "Wye",
        "AB": "Delta",
        "BC": "Delta",
        "CA": "Delta",
        "ABN": "Delta",
        "BCN": "Delta",
        "CAN": "Delta",
        "ABC": "Delta",
        "ABCN": "Delta"
    }
    return switch_dict.get(case, '')

def convert_ttranf_phases(case):
    switch_dict = {
        '0': 0,
        'M': 1,
        'B': 2,
        'T': 3,
        'MT': 1,
        'DA': 3,
        'DF': 3,
    }
    return switch_dict.get(case, 'Invalid case')


# TODO: Checar esta função
def convert_ttranf_windings(case):
    switch_dict = {
        '0': 0,
        'M': 2,
        'B': 2,
        'T': 2,
        'MT': 3,
        'DA': 2,
        'DF': 2,
    }
    return switch_dict.get(case, 'Invalid case')


def convert_tfascon_conn(case):
    switch_dict = {
        'ABCN': 'Wye',
        'ABC': 'Delta',
        'ABN': 'Wye',
        'BCN': 'Wye',
        'CAN': 'Wye',
        'AX': 'Wye',
        'BX': 'Wye',
        'CX': 'Wye',
        'AB': 'Delta',
        'BC': 'Delta',
        'CA': 'Delta',
        'AN': 'Wye',
        'BN': 'Wye',
        'CN': 'Wye',
        'A': 'Wye',
        'B': 'Wye',
        'C': 'Wye',
        'N': 'Wye',
        '0':'',
        ' ':'',
        0: ''
    }
    return switch_dict.get(case, 'Invalid case')


def convert_tpotrtv(case):
    switch_dict = {
        '0': 0,
        '1': 45,
        '2': 75,
        '3': 100,
        '4': 150,
        '5': 200,
        '6': 300,
        '7': 400,
        '8': 450,
        '9': 500,
        '10': 600,
        '11': 900,
        '12': 1200,
        '13': 1512,
        '14': 1800,
        '15': 2016,
        '16': 2400,
        '17': 3000,
        '18': 3600,
        '29': 4500,
        '19': 4800,
        '20': 5400,
        '21': 6000,
        '22': 7200,
        '23': 8400,
        '24': 9000,
        '30': 10000,
        '25': 10500,
        '26': 14000,
        '27': 15000,
        '31': 18000,
        '32': 21000,
        '28': 30000,
        '33': 36000
    }
    return switch_dict.get(case, 'Invalid case')

def convert_tpotaprt(case):
    switch_dict = {
        '0': 0,
        '1': 3,
        '2': 5,
        '3': 10,
        '4': 15,
        '5': 20,
        '6': 22.5,
        '7': 25,
        '8': 30,
        '9': 35,
        '10': 37.5,
        '11': 38.1,
        '12': 40,
        '13': 45,
        '14': 50,
        '15': 60,
        '16': 75,
        '17': 76.2,
        '18': 88,
        '19': 100,
        '20': 112.5,
        '21': 114.3,
        '22': 120,
        '23': 138,
        '24': 150,
        '25': 167,
        '26': 175,
        '27': 180,
        '28': 200,
        '29': 207,
        '30': 225,
        '31': 250,
        '32': 276,
        '33': 288,
        '34': 300,
        '35': 332,
        '36': 333,
        '37': 400,
        '38': 414,
        '39': 432,
        '40': 500,
        '41': 509,
        '42': 667,
        '43': 750,
        '44': 833,
        '45': 1000,
        '46': 1250,
        '47': 1300,
        '48': 1500,
        '49': 1750,
        '50': 2000,
        '51': 2250,
        '52': 2300,
        '53': 2400,
        '54': 2500,
        '55': 2750,
        '56': 2900,
        '57': 3000,
        '58': 3125,
        '59': 3300,
        '60': 3750,
        '61': 4000,
        '62': 4200,
        '63': 4500,
        '64': 5000,
        '65': 6250,
        '66': 6500,
        '67': 7000,
        '68': 7500,
        '69': 7800,
        '70': 8000,
        '71': 9000,
        '72': 9375,
        '73': 9600,
        '74': 10000,
        '75': 12000,
        '76': 12500,
        '77': 13300,
        '78': 15000,
        '79': 16000,
        '80': 18000,
        '81': 18750,
        '82': 20000,
        '83': 25000,
        '84': 26000,
        '85': 26600,
        '86': 28000,
        '87': 30000,
        '88': 32000,
        '89': 33000,
        '90': 33300,
        '91': 40000,
        '92': 45000,
        '93': 50000,
        '94': 60000,
        '95': 67000,
        '96': 75000,
        '97': 80000,
        '98': 83000,
        '99': 85000,
        '100': 90000,
        '101': 100000,
        '102': 200000,
        '103': 14550000,
        '104': 17320000,
        '105': 19100000,
        '106': 41550000
    }
    return switch_dict.get(case, 'Invalid case')


def qt_tipdia_mes(case, month): #inútil
    switch_dict = {
        "DU": {
            '01': 22.0,
            '02': 19.0, 
            '03': 23.0, 
            '04': 18.0, 
            '05': 22.0, 
            '06': 21.0, 
            '07': 21.0, 
            '08': 23.0, 
            '09': 20.0, 
            '10': 21.0, 
            '11': 20.0, 
            '12': 20.0
        },
        "SA": {
            '01': 4.0, 
            '02': 4.0, 
            '03': 4.0, 
            '04': 5.0, 
            '05': 4.0, 
            '06': 4.0, 
            '07': 5.0, 
            '08': 4.0, 
            '09': 5.0, 
            '10': 4.0, 
            '11': 4.0, 
            '12': 5.0
        },
        "DO": {
            '01': 5.0, 
            '02': 5.0, 
            '03': 4.0, 
            '04': 7.0, 
            '05': 5.0, 
            '06': 5.0, 
            '07': 5.0, 
            '08': 4.0, 
            '09': 5.0, 
            '10': 6.0, 
            '11': 6.0, 
            '12': 6.0
        }

    }

    if case in switch_dict and month in switch_dict[case]:
            return switch_dict[case][month]
    else:
        return 'Invalid case or month'

def convert_ptratio(case): #de acordo com o manual da bdgd e prodist modulo 10
    switch_dict = {
        '0' : 120,
        '1' : 1200,
        '18': 800,
        '2' : 600,
        '19': 300,
        '3' : 300,
        '25': 217.39,
        '4' : 217.39,
        '5': 103.75,
        '21': 200,
        '6' : 200,
        '7': 113.39,
        '8': 116.13,
        '22': 125.217,
        '9': 120,
        '10': 125.217,
        '11': 108.66,
        '12': 111.29,
        '13': 115,
        '14': 116.95,
        '23': 120,
        '15': 120,
        '16': 63.33,
        '24': 66.09,
        '17': 66.09
    }
    return switch_dict.get(case, 'Invalid case')

def convert_resist(case): #de acordo com o manual da bdgd
    switch_dict = {
        '0' : 0,
        'AL6AWG' : 2.469,
        'AL4AWG': 1.551,
        'AL3AWG' : 1.229,
        'AL2AWG': 0.975,
        'AL1AWG' : 0.774,
        'AL1_0AWG': 0.613,
        'AL2_0AWG': 0.486,
        'AL3_0AWG': 0.386,
        'AL4_0AWG': 0.306,
        'AL250MCM' : 0.259,
        'AL266_8MCM': 0.245,
        'AL300MCM': 0.217,
        'AL336_4MCM' : 0.195,
        'AL350MCM': 0.185,
        'AL397_5MCM': 0.165,
        'AL450MCM': 0.145,
        'AL477MCM': 0.138,
        'AL500MCM': 0.131,
        'AL556_5MCM': 0.119,
        'ALI10MM2': 3.514,
        'ALI16MM2': 2.179,
        'ALI25MM2': 1.369,
        'ALI35MM2': 0.991,
        'ALI50MM2': 0.732,
        'ALI70MM2': 0.506,
        'ALI95MM2': 0.365,
        'ALI120MM2': 0.289,
        'ALI150MM2': 0.236,
        'ALI185MM2': 0.188,
        'ALI240MM2': 0.143,
        'ALI300MM2': 0.115,
        'CUM0_5MM2': 40.952,
        'CUM0_75MM2': 27.87,
        'CUM1MM2': 20.59,
        'CUM1_5MM2': 13.764,
        'CUM2_5MM2': 8.429,
        'CUM4MM2': 5.244,
        'CUM6MM2': 3.504,
        'CUM10MM2': 2.082,
        'CUM16MM2': 1.308,
        'CUM25MM2': 0.827,
        'CUM35MM2': 0.596,
        'CUM50MM2': 0.441,
        'CUM70MM2': 0.305,
        'CUM95MM2': 0.22,
        'CUM120MM2': 0.175,
        'CUM150MM2': 0.142,
        'CUM185MM2': 0.114,
        'CUM240MM2': 0.087,
        'CUM300MM2': 0.07,
        'CUM400MM2': 0.056,
        'CUM500MM2': 0.044,
        'CUM630MM2': 0.036,
        'CUM800MM2': 0.029,
        'CUM1000MM2': 0.025,
        'CUM1200MM2': 0.022,
        'CUM1400MM2': 0.02,
        'CUM1600MM2': 0.019,
        'CUM1800MM2': 0.017,
        'CUM2000MM2': 0.016,
        'CU10AWG': 3.754,
        'CU9AWG': 2.958,
        'CU8AWG': 2.389,
        'CU7AWG': 1.82,
        'CU6AWG': 1.564,
        'CU5AWG': 1.138,
        'CU4AWG': 0.984,
        'CU3AWG': 0.78,
        'CU2AWG': 0.62,
        'CU1AWG': 0.491,
        'CU1_0AWG': 0.389,
        'CU2_0AWG': 0.308,
        'CU3_0AWG': 0.245,
        'CU4_0AWG': 0.195,
        'AZN1X3_09MM': 29.142,
        'AZN3X2_25MM': 18.325,
        'AZN5X6MM': 11.411,
        'AAL7X9AWG': 2.1,
        'AAL7X8AWG': 1.673,
        'AAL7X7AWG': 1.32,
        'AAL7X6AWG': 1.047,
        'AAL7X5AWG': 0.833,
        'AAL7X10AWG': 2.651,
        'AAL3X9AWG': 4.873,
        'AAL3X8AWG': 3.883,
        'AAL3X7AWG': 3.064,
        'AAL3X6AWG': 2.432,
        'AAL3X5AWG': 1.934,
        'AAL3X10AWG': 6.153
    }
    return switch_dict.get(case, 'Invalid case')
