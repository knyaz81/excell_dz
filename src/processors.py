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


    def __init__(self, filename, copy_from):
        self.parser = XLSXParser(filename)
        self.db = DataBase()
        self.copy_from = copy_from

    def run_process(self):
        self.db.init_db()
        self.db.create_tables()

        overall_start_time = time()
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

        if self.copy_from:
            self.prods_file_obj = StringIO()
            self.prod_attrs_file_obj = StringIO()
            for category in self.categories:
                self._fill_prods_and_attrs_file_obj(category)
        else:
            self.products = []
            self.product_attributes = []
            for category in self.categories:
                self._fill_products_and_attrs_values_list(category)

        self.db.bulk_create(
            TABLENAME_BRANDS,
            BRANDS_FIELDS,
            [(self.brands[brand], brand) for brand in self.brands]
        )

        writing_start_time = time()
        if self.copy_from:
            self.db.create_from_file(
                TABLENAME_PRODUCTS, PRODUCT_FIELDS, self.prods_file_obj
            )
            self.db.create_from_file(
                TABLENAME_PRODUCT_ATTRIBUTES,
                PRODUCT_ATTRIBUTE_FIELDS,
                self.prod_attrs_file_obj
            )
        else:
            self.db.bulk_create(
                TABLENAME_PRODUCTS,
                PRODUCT_FIELDS,
                self.products
            )

            self.db.bulk_create(
                TABLENAME_PRODUCT_ATTRIBUTES,
                PRODUCT_ATTRIBUTE_FIELDS,
                self.product_attributes
            )

        print(f'Write data to database in {(time()-writing_start_time):.3f}sec')
        print(f'Document parse DONE in {(time()-overall_start_time):.3f}sec')

    def _fill_products_and_attrs_values_list(self, category):
        attribute_names = self.parser.get_adv_attrs_by_category(category)
        brand_id_gen = iter_count(start=1)
        for product_values in self.parser.generator_parse_proructs_row(category):

            if product_values[self.PRODUCT_BRAND_INDEX] not in self.brands:
                self.brands[product_values[self.PRODUCT_BRAND_INDEX]] = next(brand_id_gen)

            product = (
                self.categories[category],
                self.brands[product_values[self.PRODUCT_BRAND_INDEX]],
                product_values[self.PRODUCT_CODE_INDEX],
                product_values[self.PRODUCT_NAME_INDEX],
                product_values[self.PRODUCT_PRICE_INDEX],
            )
            self.products.append(product)

            for i, prod_values_idx in enumerate(range(4, len(product_values))):
                prod_attribute = (
                    product_values[self.PRODUCT_CODE_INDEX],
                    self.attributes[attribute_names[i]],
                    product_values[prod_values_idx] 
                )
                self.product_attributes.append(prod_attribute)

    def _fill_prods_and_attrs_file_obj(self, category):
        attribute_names = self.parser.get_adv_attrs_by_category(category)
        brand_id_gen = iter_count(start=1)
        for product_values in self.parser.generator_parse_proructs_row(category):

            if product_values[self.PRODUCT_BRAND_INDEX] not in self.brands:
                self.brands[product_values[self.PRODUCT_BRAND_INDEX]] = next(brand_id_gen)

            self.prods_file_obj.write(
                "{}\t{}\t{}\t{}\t{}\n".format(
                    self.categories[category],
                    self.brands[product_values[self.PRODUCT_BRAND_INDEX]],
                    product_values[self.PRODUCT_CODE_INDEX],
                    product_values[self.PRODUCT_NAME_INDEX],
                    product_values[self.PRODUCT_PRICE_INDEX],
                )
            )

            for i, prod_values_idx in enumerate(range(4, len(product_values))):
                self.prod_attrs_file_obj.write(
                    "{}\t{}\t{}\n".format(
                        product_values[self.PRODUCT_CODE_INDEX],
                        self.attributes[attribute_names[i]],
                        product_values[prod_values_idx]
                    )
                )




def runner(cli_args):
    processor = CommonProcessor(cli_args.file, cli_args.copyfrom)
    processor.run_process()


class AsyncProcessor:
    pass