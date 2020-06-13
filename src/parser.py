from time import time
from openpyxl import load_workbook

from src.database import DataBase

class XLSXParser:
    def __init__(self, filename):
        self.workbook = load_workbook(filename)
        self.worksheets = self.workbook.worksheets

    def parse_categories_names(self):
        return [sheet.title for sheet in self.worksheets]

    def parse_addvanced_attributes_names(self):
        attributes = set()
        for sheet in self.worksheets:
            for column in range(6, sheet.max_column + 1):
                attributes |= {sheet.cell(row=2, column=column).value}
        return list(attributes)

    def generator_parse_proructs_row(self, category):
        sheet = self.workbook[category]

        for row in range(3, sheet.max_row + 1):
            yield [
                sheet.cell(row=row, column=column).value for column in range(2, sheet.max_column + 1)
            ]

    def get_adv_attrs_by_category(self, category):
        sheet = self.workbook[category]
        return [sheet.cell(row=2, column=column).value for column in range(6, sheet.max_column + 1)]





