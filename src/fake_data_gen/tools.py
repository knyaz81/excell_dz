import random
from string import ascii_uppercase

_RATIO = ['11:9', '16:10', '16:9', '19:10', '21:9', '25:16', '3:2', '4:3', '5:3', '5:4', '64:27']

_RESOLUTION = [
    '1280 × 768', '1280 × 1024', '1408 × 1152', '1440 × 900', '1400 × 1050', '1440 × 1080', '1536 × 960',
    '1536 × 1024', '1600 × 900', '1600 × 1024', '1600 × 1200', '1680 × 1050', '1920 × 1080', '1920 × 1200',
    '2048 × 1080', '2048 × 1152', '2048 × 1536', '2560 × 1080', '2560 × 1440', '2560 × 1600', '2560 × 2048',
    '3200 × 1800', '3200 × 2048', '3200 × 2400', '3440 × 1440', '3840 × 2160', '3840 × 2400', '4096 × 2160',
    '5120 × 2880', '5120 × 4096', '6400 × 4096', '6400 × 4800', '7680 × 4320', '7680 × 4800',
 ]

class ExcellGenTools:
    ATTRIBUT_LIST = 'attrs'
    CAT_NAME = 'name'
    RANDOM_VALUE_FUNC = 'rand_func'
    COLUMN_WIDTH = 'col_width'
    _current_article = 0

    CATEGORIES = {
        'TV': {
            'name': 'TV',
            'attrs': ['Weight', 'Diagonal'],
        },
        'Camera': {
            'name': 'CAM',
            'attrs': ['Weight', 'Number of pixels'],
        },
        'Dysplay': {
            'name': 'DS',
            'attrs': ['Diagonal', 'Resolution', 'Screen ratio'],
        },
        'Mobile': {
            'name': 'MOB',
            'attrs': ['Diagonal', 'ROM'],
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
            'col_width': 15,
        },
        'Resolution': {
            'rand_func': lambda: '{}'.format(random.choice(_RESOLUTION)),
            'col_width': 15,
        },
        'Screen ratio': {
            'rand_func': lambda: '{}'.format(random.choice(_RATIO)),
            'col_width': 11,
        },
        'ROM': {
            'rand_func': lambda: '{}GB'.format(pow(2, random.randint(3, 9))),
            'col_width': 0,
        },
    }

    PRODUCTS = {
        'Article': {
            'col_width': 14,
        },
        'Brand': {
            'col_width': 11,
        },
        'Name': {
            'col_width': 30,
        },
        'Price': {
            'col_width': 11,
        },
    }

    COLUMNS = ['', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']

    @classmethod
    def get_column_width(cls, attr):
        if attr in cls.ATTRS:
            return cls.ATTRS[attr][cls.COLUMN_WIDTH]
        return cls.PRODUCTS[attr][cls.COLUMN_WIDTH]

    @classmethod
    def get_attribute_list(cls, category):
        return cls.CATEGORIES[category][cls.ATTRIBUT_LIST]

    @classmethod
    def get_article(cls):
        cls._current_article += random.randint(1, 30)
        return "A-{:0>8}".format(cls._current_article)

    @classmethod
    def get_brand(cls):
        return random.choice(cls.BRANDS)

    @classmethod
    def get_name(cls, category, brand):
        return "{brand}{index:0>6}{category}-{suffix}".format(
            category=category,
            brand=brand,
            index=random.randint(123, 987654),
            suffix=random.choice(ascii_uppercase) + random.choice(ascii_uppercase),
        )

    @classmethod
    def get_price(cls):
        return "${:.2f}".format(random.randint(19999, 129999) / 100)

    @classmethod
    def get_attribute_value(cls, attr):
        return cls.ATTRS[attr][cls.RANDOM_VALUE_FUNC]()

