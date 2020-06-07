import random

_RATIO = ['11:9', '16:10', '16:9', '19:10', '21:9', '25:16', '3:2', '4:3', '5:3', '5:4', '64:27']

_RESOLUTION = [
    '1280 × 768', '1280 × 1024', '1408 × 1152', '1440 × 900', '1400 × 1050', '1440 × 1080', '1536 × 960',
    '1536 × 1024', '1600 × 900', '1600 × 1024', '1600 × 1200', '1680 × 1050', '1920 × 1080', '1920 × 1200',
    '2048 × 1080', '2048 × 1152', '2048 × 1536', '2560 × 1080', '2560 × 1440', '2560 × 1600', '2560 × 2048',
    '3200 × 1800', '3200 × 2048', '3200 × 2400', '3440 × 1440', '3840 × 2160', '3840 × 2400', '4096 × 2160',
    '5120 × 2880', '5120 × 4096', '6400 × 4096', '6400 × 4800', '7680 × 4320', '7680 × 4800',
 ]

class ExcellGenTools:
    ATRIBUT_LIST = 'attrs'
    CAT_NAME = 'name'
    RANDOM_VALUE_FUNC = 'rand_func'
    COLUMN_WIDTH = 'col_width'

    CATEGORIES = {
        'TV': {
            'name': 'TV',
            'attrs': ['Weight', 'Diagonal']
        },
        'Camera': {
            'name': 'CAM',
            'attrs': ['Weight', 'Number of pixels']
        },
        'Dysplay': {
            'name': 'DS',
            'attrs': ['Diagonal', 'Resolution', 'Screen ratio']
        },
        'Mobile': {
            'name': 'MOB',
            'attrs': ['Diagonal', 'ROM']
        },
    }

    BRANDS = [
        'SONY',
        'PANASONIC',
        'SAMSUNG',
        'LG',
        'SIEMENS',
        'XIAOMI',
    ]

    ATTRS = {
        'Weight': {
            'rand_func': lambda: '{:.2f}kg'.format(random.random()*10),
            'col_width': 0,
        },
        'Diagonal': {
            'rand_func': lambda: '{}"'.format(random.randint(6, 42)),
            'col_width': 0,
        },
        'Number of pixels': {
            'rand_func': lambda: '{}MP'.format(random.randint(30, 300) / 10),
            'col_width': 18,
        },
        'Resolution': {
            'rand_func': lambda: '{}'.format(random.choice(_RESOLUTION)),
            'col_width': 20,
        },
        'Screen ratio': {
            'rand_func': lambda: '{}'.format(random.choice(_RATIO)),
            'col_width': 18,
        },
        'ROM': {
            'rand_func': lambda: '{}GB'.format(pow(2, random.randint(3, 9))),
            'col_width': 0,
        },
    }

    PRODUCTS = [
        'Article',
        'Brand',
        'Name',
        'Price',
    ]

    COLUMNS = ['', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']