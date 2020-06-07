#! /usr/bin/env python
import sys

from src.core.settings import ACTIONS
from src.core.args import args_parser

def main():
    print('Python v', sys.version, sep='')
    print('Excell parser application.\n')

    ARGUMENTS, _ = args_parser.parse_known_args()

    if ARGUMENTS.action == 'help':
        args_parser.print_help()
    elif ARGUMENTS.action not in ACTIONS:
        print(f'***WARNING*** Unknown action {ARGUMENTS.action} \n')
        args_parser.print_help()
        sys.exit(1)
    else:
        from src.app import app
        app(ARGUMENTS)

if __name__ == '__main__':
    main()