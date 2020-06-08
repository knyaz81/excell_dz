from excell_gen.starter import GeneratorXLSX
from xlsx_parser.parser import XLSXParser

PROGRAM_ACTIONS = {
    'generate': GeneratorXLSX(),
    'parse_excell' : XLSXParser(),
}


def app(cli_args):
    PROGRAM_ACTIONS[cli_args.action](cli_args)
