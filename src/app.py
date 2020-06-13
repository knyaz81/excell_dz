from src.excell_gen.starter import GeneratorXLSX
from src.processors import runner

PROGRAM_ACTIONS = {
    'generate': GeneratorXLSX(),
    'parse' : runner,
}


def app(cli_args):
    PROGRAM_ACTIONS[cli_args.action](cli_args)
