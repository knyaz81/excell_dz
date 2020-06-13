from itertools import count as iter_count
from io import StringIO
from time import time

from src.parser import XLSXParser
from src.database import DataBase
from src.core.settings import (
    CATEGORY_FIELDS,
    BRANDS_FIELDS,
    ATTRIBUTE_FIELDS,
    PRODUCT_FIELDS,
    PRODUCT_ATTRIBUTE_FIELDS,
    TABLENAME_CATEGORIES,
    TABLENAME_ATTRIBUTES,
    TABLENAME_BRANDS,
    TABLENAME_PRODUCTS,
    TABLENAME_PRODUCT_ATTRIBUTES,
)

class CommonProcessor:

    PRODUCT_BRAND_INDEX = 1
    PRODUCT_CODE_INDEX = 0
    PRODUCT_NAME_INDEX = 2
    PRODUCT_PRICE_INDEX = 3


    def __init__(self, filename):
        self.parser = XLSXParser(filename)
        self.db = DataBase()

    def run_process(self):
        self.db.init_db()
        self.db.create_tables()

        starting_time = time()
        self.categories = {
            category: id for category, id in zip(self.parser.parse_categories_names(), iter_count(start=1))
        }
        self.attributes = {
            attribute: id for attribute, id in zip(self.parser.parse_addvanced_attributes_names(), iter_count(start=1))
        }
        self.brands = {}

        self.db.bulk_create(
            TABLENAME_CATEGORIES,
            CATEGORY_FIELDS,
            [(self.categories[category], category) for category in self.categories]
        )
        self.db.bulk_create(
            TABLENAME_ATTRIBUTES,
            ATTRIBUTE_FIELDS,
            [(self.attributes[attribute], attribute) for attribute in self.attributes]
        )

        for category in self.categories:
            products, product_attributes = self._get_products_and_attrs(category)
            

        self.db.bulk_create(
            TABLENAME_BRANDS,
            BRANDS_FIELDS,
            [(self.brands[brand], brand) for brand in self.brands]
        )

        self.db.bulk_create(
            TABLENAME_PRODUCTS,
            PRODUCT_FIELDS,
            products
        )

        self.db.bulk_create(
            TABLENAME_PRODUCT_ATTRIBUTES,
            PRODUCT_ATTRIBUTE_FIELDS,
            product_attributes
        )
        print(f'DOCUMENT PARSE DONE in {(time()-starting_time):.3f}sec')

    def _get_products_and_attrs(self, category):
        products = []
        product_attributes = []
        attribute_names = self.parser.get_adv_attrs_by_category(category)
        brand_id_gen = iter_count(start=1)
        for product_values in self.parser.generator_parse_proructs_row(category):
            if product_values[self.PRODUCT_BRAND_INDEX] not in self.brands:
                self.brands[product_values[self.PRODUCT_BRAND_INDEX]] = next(brand_id_gen)
            products.append(
                (
                    self.categories[category],
                    self.brands[product_values[self.PRODUCT_BRAND_INDEX]],
                    product_values[self.PRODUCT_CODE_INDEX],
                    product_values[self.PRODUCT_NAME_INDEX],
                    product_values[self.PRODUCT_PRICE_INDEX],
                )
            )
            for i, prod_values_idx in enumerate(range(4, len(product_values))):
                product_attributes.append(
                    (
                       product_values[self.PRODUCT_CODE_INDEX],
                       self.attributes[attribute_names[i]],
                       product_values[prod_values_idx] 
                    )
                )
        return (products, product_attributes)

def runner(cli_args):
    processor = CommonProcessor(cli_args.file)
    processor.run_process()


class AsyncProcessor:
    pass