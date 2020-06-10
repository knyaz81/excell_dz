import asyncio

import openpyxl
import uvloop

from database.engine import DataBase, AsyncPostgreSQL

# операция чтения xlsx блокирующая, поэтому читаем понемногу
MAX_RAWS_READ = 100

class AsyncXLSXParser:
    def __init__(self, loop, cli_args):
        self.cli_args = cli_args
        self.loop = loop
        self.workbook = openpyxl.load_workbook(self.cli_args.file)
        self.worksheets = self.workbook.worksheets
        self.categories = [ws.title for ws in self.worksheets]
        self.brand_ids = {}
        self.prod_attr_ids = {}


    async def __call__(self):
        await asyncio.gather(
            *[self.parse_category(sheet) for sheet in self.worksheets],
            return_exceptions=True
        )

    async def parse_category(self, sheet):
        database = AsyncPostgreSQL()
        async with database:
            category_id = await database.insert_category(sheet.title)

        category_attrs_list = []
        for column in range(6, sheet.max_column + 1):
            attr = sheet.cell(row=2, column=column).value
            with asyncio.Lock():
                await self._check_attribute(attr, database)
            category_attrs_list.append(self.prod_attr_ids[attr])

    async def _check_attribute(self, attribute, database):
        if attribute not in self.prod_attr_ids:
            async with database:
                self.prod_attr_ids[attribute] = await database.insert_attribut(attribute)

    async def _check_brand(self, brand, database):
        if brand not in self.brand_ids:
            async with database:
                self.brand_ids[brand] =  await database.insert_brand(brand)


def run(cli_args):
    uvloop.install()
    loop = asyncio.get_event_loop()
    parser = AsyncXLSXParser(loop, cli_args)
    loop.run_until_complete(parser())