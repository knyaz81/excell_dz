#! /usr/bin/env python
import sys

def main():
    print('Python v', sys.version, sep='')
    print('Excell parser application.\n')

    from src.core.args import args_parser
    ARGUMENTS, _ = args_parser.parse_known_args()

    if ARGUMENTS.action == 'help':
        args_parser.print_help()

    from src.core import settings
    if ARGUMENTS.action not in settings.ACTIONS:
        print(f'***WARNING*** Unknown action {ARGUMENTS.action} \n')
        args_parser.print_help()
        sys.exit(1)

    from src.app import app
    app(ARGUMENTS.action)

if __name__ == '__main__':
    main()