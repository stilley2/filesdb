import argparse
import os

from ._filesdb import add, search, delete, merge, _print_rows, _parse_metadata


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', '--database', type=str, default='files.db', help='Name of database file')
    parser.add_argument('--wd', '--working_directory', type=str, default='.')
    parser.add_argument('--timeout', type=float, default=10.0)
    subparsers = parser.add_subparsers()

    parser_search = subparsers.add_parser('search', help='Search database')
    parser_search.add_argument('-d', '--delimiter', type=str, default='\t', help='Output column delimiter')
    parser_search.add_argument('-o', '--output_columns', type=str, default=None, help='Comma delimited list of column names to print')
    parser_search.add_argument('metadata', nargs='*', help='list of keys and values', metavar="KEY=VALUE")
    parser_search.set_defaults(subcommand='search')

    parser_add = subparsers.add_parser('add', help=('Add file to database. If filename is not specified, ' +
                                                    'create a unique file name with extension given by ext'))
    parser_add.add_argument('--filename', type=str)
    parser_add.add_argument('--prefix', type=str, default='')
    parser_add.add_argument('--suffix', type=str, default='')
    parser_add.add_argument('--ext', type=str, default='')
    parser_add.add_argument('metadata', nargs='*', help='List of keys and values.', metavar="KEY=VALUE")
    parser_add.set_defaults(subcommand='add')

    parser_delete = subparsers.add_parser('delete', help='Delete files from database and working director')
    parser_delete.add_argument('-n', '--dry_run', action='store_true', help='Print entries to be delete, but do not delete')
    parser_delete.add_argument('-d', '--delimiter', type=str, default='\t', help='Output column delimiter for dry run')
    parser_delete.add_argument('-o', '--output_columns', type=str, default=None, help='Comma delimited list of column names to print')
    parser_delete.add_argument('metadata', nargs='*', help='List of keys and values', metavar="KEY=VALUE")
    parser_delete.set_defaults(subcommand='delete')

    parser_merge = subparsers.add_parser('merge', help='Merge input database into --db. (input remains unchanged)')
    parser_merge.add_argument('input', type=str, help='Input database.')
    parser_merge.set_defaults(subcommand='merge')

    parser_test = subparsers.add_parser('test', help='Run tests')
    parser_test.set_defaults(subcommand='test')

    args = parser.parse_args()

    if 'subcommand' not in vars(args).keys():
        parser.print_help()

    elif args.subcommand == 'search':
        metadata = _parse_metadata(args.metadata)
        _print_rows(search(metadata, db=args.db, wd=args.wd, timeout=args.timeout), delimiter=args.delimiter,
                    keys=None if args.output_columns is None else args.output_columns.split(','))

    elif args.subcommand == 'add':
        metadata = _parse_metadata(args.metadata)
        filename = add(metadata, db=args.db, wd=args.wd, filename=args.filename, timeout=args.timeout, ext=args.ext,
                       prefix=args.prefix, suffix=args.suffix)
        print(filename)

    elif args.subcommand == 'delete':
        metadata = _parse_metadata(args.metadata)
        rows = delete(metadata, db=args.db, wd=args.wd, timeout=args.timeout, dryrun=args.dry_run)
        _print_rows(rows, delimiter=args.delimiter,
                    keys=None if args.output_columns is None else args.output_columns.split(','))

    elif args.subcommand == 'merge':
        merge(args.input, args.db, wd=args.wd)

    elif args.subcommand == 'test':
        import pytest
        pytest.main([os.path.split(__file__)[0]])

    else:
        raise RuntimeError


if __name__ == '__main__':
    main()
