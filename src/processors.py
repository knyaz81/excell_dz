import asyncio
from itertools import count as iter_count
from io import StringIO
from time import time

from aiojobs import create_scheduler

from src.parser import XLSXParser
from src.database import DataBase, AsyncDataBase
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
    MAX_DB_CONNECTION,
)

PRODUCT_BRAND_INDEX = 1
PRODUCT_CODE_INDEX = 0
PRODUCT_NAME_INDEX = 2
PRODUCT_PRICE_INDEX = 3

class CommonProcessor:
    def __init__(self, filename, copy_from):
        self.parser = XLSXParser(filename)
        self.copy_from = copy_from
        self.db = DataBase()
        self.db.init_db()
        self.db.create_tables()

    def run_process(self):
        overall_start_time = time()
        self.categories = {
            category: id for category, id in zip(
                self.parser.parse_categories_names(), iter_count(start=1)
            )
        }
        self.attributes = {
            attribute: id for attribute, id in zip(
                self.parser.parse_addvanced_attributes_names(), iter_count(start=1)
            )
        }
        self.brands = {
            brand: id for brand, id in zip(
                self.parser.parse_brands_names(), iter_count(start=1)
            )
        }

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
        self.db.bulk_create(
            TABLENAME_BRANDS,
            BRANDS_FIELDS,
            [(self.brands[brand], brand) for brand in self.brands]
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
        for product_values in self.parser.generator_parse_proructs_row(category):
            product = (
                self.categories[category],
                self.brands[product_values[PRODUCT_BRAND_INDEX]],
                product_values[PRODUCT_CODE_INDEX],
                product_values[PRODUCT_NAME_INDEX],
                product_values[PRODUCT_PRICE_INDEX],
            )
            self.products.append(product)

            for i, prod_values_idx in enumerate(range(4, len(product_values))):
                prod_attribute = (
                    product_values[PRODUCT_CODE_INDEX],
                    self.attributes[attribute_names[i]],
                    product_values[prod_values_idx] 
                )
                self.product_attributes.append(prod_attribute)

    def _fill_prods_and_attrs_file_obj(self, category):
        attribute_names = self.parser.get_adv_attrs_by_category(category)
        for product_values in self.parser.generator_parse_proructs_row(category):
            self.prods_file_obj.write(
                "{}\t{}\t{}\t{}\t{}\n".format(
                    self.categories[category],
                    self.brands[product_values[PRODUCT_BRAND_INDEX]],
                    product_values[PRODUCT_CODE_INDEX],
                    product_values[PRODUCT_NAME_INDEX],
                    product_values[PRODUCT_PRICE_INDEX],
                )
            )

            for i, prod_values_idx in enumerate(range(4, len(product_values))):
                self.prod_attrs_file_obj.write(
                    "{}\t{}\t{}\n".format(
                        product_values[PRODUCT_CODE_INDEX],
                        self.attributes[attribute_names[i]],
                        product_values[prod_values_idx]
                    )
                )


class AsyncProcessor:

    CHUNK_READ_RAWS = 250

    def __init__(self, filename, loop):
        self.loop = loop
        self.parser = XLSXParser(filename)
        self.sync_db = DataBase()
        self.sync_db.init_db()
        self.sync_db.create_tables()


    async def run_process(self):
        self.scheduler = await create_scheduler(limit=MAX_DB_CONNECTION)

        self.categories = {
            category: id for category, id in zip(
                self.parser.parse_categories_names(), iter_count(start=1)
            )
        }
        self.attributes = {
            attribute: id for attribute, id in zip(
                self.parser.parse_addvanced_attributes_names(), iter_count(start=1)
            )
        }
        self.brands = {
            brand: id for brand, id in zip(
                self.parser.parse_brands_names(), iter_count(start=1)
            )
        }

        await self.scheduler.spawn(
            self._simple_bulk_create(
                TABLENAME_CATEGORIES,
                CATEGORY_FIELDS,
                self.categories
            )
        )
        await self.scheduler.spawn(
            self._simple_bulk_create(
                TABLENAME_ATTRIBUTES,
                ATTRIBUTE_FIELDS,
                self.attributes
            )
        )
        await self.scheduler.spawn(
            self._simple_bulk_create(
                TABLENAME_BRANDS,
                BRANDS_FIELDS,
                self.brands
            )
        )

        await asyncio.gather(*[self.parse_category(category) for category in self.categories])

        pending = asyncio.Task.all_tasks()
        await asyncio.gather(*pending)

    async def parse_category(self, category):
            products = []
            advanced_attributes = []
            attribute_names = self.parser.get_adv_attrs_by_category(category)
            for row_quantity, product_values in enumerate(self.parser.generator_parse_proructs_row(category), start=1):
                brand_name = product_values[PRODUCT_BRAND_INDEX]             
                products.append(
                    (
                        self.categories[category],
                        self.brands[brand_name],
                        product_values[PRODUCT_CODE_INDEX],
                        product_values[PRODUCT_NAME_INDEX],
                        product_values[PRODUCT_PRICE_INDEX],
                    )
                )
                for i, prod_values_idx in enumerate(range(4, len(product_values))):
                    prod_attribute = (
                        product_values[PRODUCT_CODE_INDEX],
                        self.attributes[attribute_names[i]],
                        product_values[prod_values_idx] 
                    )
                    advanced_attributes.append(prod_attribute)

                if not row_quantity % self.CHUNK_READ_RAWS:
                    await self.scheduler.spawn(
                        self._create_products_and_attrs(products, advanced_attributes)
                    )
                    products = []
                    advanced_attributes = []
            if products and advanced_attributes:
                await self.scheduler.spawn(
                    self._create_products_and_attrs(products, advanced_attributes)
                )

    async def _simple_bulk_create(self, tablename, fields, values_dict):
        database = AsyncDataBase()
        async with database:
            await database.bulk_create(
                tablename,
                fields,
                [(values_dict[name], name) for name in values_dict]
            )

    async def _simple_create(self, tablename, fields, values):
        database = AsyncDataBase()
        async with database:
            await database.create(
                tablename,
                fields,
                values
            )

    async def _create_products_and_attrs(self, products, attributes):
        database = AsyncDataBase()
        async with database:
            await database.bulk_create(
                TABLENAME_PRODUCTS,
                PRODUCT_FIELDS,
                products
            )
            await database.bulk_create(
                TABLENAME_PRODUCT_ATTRIBUTES,
                PRODUCT_ATTRIBUTE_FIELDS,
                attributes
            )
