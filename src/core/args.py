import argparse

from .settings import ACTIONS

args_parser =  argparse.ArgumentParser()

args_parser.add_argument(
    'action', type=str, nargs='?', default='help',
    help=(
        "Action to execute, required positional argument. Use: "
        f"{', '.join(ACTIONS)}"
    )
)

args_parser.add_argument(
    '-f', '--file', type=str, default='temp_dz.xlsx',
    help='full excell filename, e.g. "-f example.xslx", default "temp_dz.xlsx"'
)
args_parser.add_argument(
    '-r', '--rows', type=int, default=40,
    help='Rows products per category in xlsx document, default 40'
)
args_parser.add_argument(
    '-b', '--bulk', type=bool, default=False, nargs='?', const=True,
    help='Write bulk data to SQL BD'
)
