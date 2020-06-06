#! /usr/bin/env python
import sys

def main():
    print('Python v', sys.version, sep='')
    print('Excell parser application.\n')

    from src.core.args import args_parser
    ARGUMENTS, _ = args_parser.parse_known_args()

    args_parser.print_help()

if __name__ == '__main__':
    main()