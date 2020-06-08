import openpyxl

from database.engine import DataBase

class XLSXParser:

    def __call__(self, cli_args):
        self.filename = cli_args.file
        self.workbook = openpyxl.load_workbook(self.filename)
        self.worksheets = self.workbook.worksheets
        self.categories = [ws.title for ws in self.worksheets]
        self.database = DataBase()

        self.database.init_db()
        self.database.create_tables()
        self.database.insert_categories([(cat,) for cat in self.categories])
