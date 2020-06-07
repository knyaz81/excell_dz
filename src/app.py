from .excell_gen.starter import GeneratorXLSX

PROGRAM_ACTIONS = {
    'generate': GeneratorXLSX(),
}


def app(cli_args):
    PROGRAM_ACTIONS[cli_args.action](cli_args)
