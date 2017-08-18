from __future__ import absolute_import
from __future__ import print_function

import argparse
import datetime
import os
import sqlite3
import uuid


# https://stackoverflow.com/questions/305378/list-of-tables-db-schema-dump-etc-using-the-python-sqlite3-api
# https://stackoverflow.com/questions/25387537/sqlite3-operationalerror-near-syntax-error
# https://stackoverflow.com/questions/15647580/does-null-have-a-data-type
# https://stackoverflow.com/questions/7519621/where-is-null-not-working-in-sqlite
# https://stackoverflow.com/questions/16856647/sqlite3-programmingerror-incorrect-number-of-bindings-supplied-the-current-sta


RESERVED_KEYS = "filename", "time"


def _key_val_list(d, separate_nulls=False):
    keys = []
    vals = []
    null_list = []
    for key, val in d.items():
        if separate_nulls and val is None:
            null_list.append(key)
        else:
            keys.append(key)
            vals.append(val)
    if separate_nulls:
        return keys, vals, null_list
    else:
        return keys, vals


def _get_conn(db, wd, timeout=10):
    conn = sqlite3.connect(os.path.join(wd, db), timeout=timeout)
    conn.row_factory = sqlite3.Row
    with conn:
        conn.execute("create table if not exists filelist (filename text primary key not null, time timestamp)")
    return conn


def add(metadata, db="files.db", wd='.', filename=None, timeout=10, ext=None):
    for reserved_key in RESERVED_KEYS:
        if reserved_key in metadata.keys():
            raise ValueError("filename is reserved")
    conn = _get_conn(db, wd, timeout=timeout)
    with conn:
        desc = conn.execute("select * from filelist").description
        columns = [d[0] for d in desc]
        for key in metadata.keys():
            if key not in columns:
                try:
                    conn.execute("alter table filelist add {} NUMERIC".format(key))
                except sqlite3.OperationalError:
                    # column already exists. possible due to race condition between populating
                    # columns variable and adding the new column
                    pass
        keys, vals = _key_val_list(metadata)
        if filename is None:
            filename = "{}".format(uuid.uuid4())
            if ext is not None:
                filename += '.{}'.format(ext)
        conn.execute("insert into filelist (filename, time, " + ', '.join(keys) + ") values (" + ', '.join(["?"] * (len(vals) + 2)) + ")", [filename, datetime.datetime.now()] + vals)
    return filename


def _make_expression_vals(metadata):
    keys, vals, null_keys = _key_val_list(metadata, separate_nulls=True)
    search_strs = []
    if len(keys) > 0:
        search_strs.append(" and ".join(["{}=?".format(key) for key in keys]))
    if len(null_keys) > 0:
        search_strs.append(" and ".join(["{} is null".format(key) for key in null_keys]))
    expr = " and ".join(search_strs)
    return expr, vals


def search(metadata, db="files.db", wd='.', timeout=10):
    conn = _get_conn(db, wd, timeout=timeout)
    with conn:
        if len(metadata) > 0:
            expr, vals = _make_expression_vals(metadata)
            query_string = "select * from filelist where " + expr
            rows = conn.execute(query_string, vals).fetchall()
        else:
            rows = conn.execute("select * from filelist").fetchall()
    return rows


def print_csv(db='files.db', wd='.', timeout=10):
    rows = search({}, db=db, wd=wd, timeout=timeout)
    first = True
    for row in rows:
        if first:
            keys = list(row.keys())
            print(','.join(keys))
            first = False
        print(','.join([str(row[key]) for key in keys]))


def delete(metadata, db='files.db', wd='.', timeout=10, dryrun=False, delimiter='\t'):
    if len(metadata) == 0:
        raise ValueError("must have at least one search parameter")
    conn = _get_conn(db, wd, timeout=timeout)
    with conn:
        rows = search(metadata, db=db, wd=wd, timeout=timeout)
        if len(rows) > 0:
            if not dryrun:
                for r in rows:
                    if os.path.exists(os.path.join(wd, r['filename'])):
                        os.remove(os.path.join(wd, r['filename']))
                    # potential race condition here, but delete should not be called often
                    # and it should be called directly (not in a script) so the user can
                    # keep track of potential issues.
                    conn.execute('delete from filelist where filename=?', (r['filename'],))
    return rows


def _parse_metadata(metadatalist):
    metadata = {}
    for entry in metadatalist:
        key, val = entry.split('=')
        val.strip()
        if val == 'None':
            val = None
        metadata[key.strip()] = val
    return metadata


def _print_rows(rows, delimiter='\t'):
    first = True
    for row in rows:
        if first:
            keys = list(row.keys())
            print(delimiter.join(keys))
            first = False
        print(delimiter.join([str(row[key]) for key in keys]))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', '--database', type=str, default='files.db', help='Name of database file')
    parser.add_argument('--wd', '--working_directory', type=str, default='.')
    parser.add_argument('--timeout', type=float, default=10.0)
    subparsers = parser.add_subparsers()

    parser_search = subparsers.add_parser('search', help='Search database')
    parser_search.add_argument('-d', '--delimiter', type=str, default='\t', help='Output column delimiter')
    parser_search.add_argument('metadata', nargs='*', help='list of keys and values', metavar="KEY=VALUE")
    parser_search.set_defaults(subcommand='search')

    parser_add = subparsers.add_parser('add', help=('Add file to database. If filename is not specified, ' +
                                                    'create a unique file name with extension given by ext'))
    parser_add.add_argument('--filename', type=str)
    parser_add.add_argument('--ext', type=str)
    parser_add.add_argument('metadata', nargs='*', help='List of keys and values', metavar="KEY=VALUE")
    parser_add.set_defaults(subcommand='add')

    parser_delete = subparsers.add_parser('delete', help='Delete files from database and working director')
    parser_delete.add_argument('-n', '--dry_run', action='store_true', help='Print entries to be delete, but do not delete')
    parser_delete.add_argument('-d', '--delimiter', type=str, default='\t', help='Output column delimiter for dry run')
    parser_delete.add_argument('metadata', nargs='*', help='List of keys and values', metavar="KEY=VALUE")
    parser_delete.set_defaults(subcommand='delete')

    parser_test = subparsers.add_parser('test', help='Run tests')
    parser_test.set_defaults(subcommand='test')

    args = parser.parse_args()

    if 'subcommand' not in vars(args).keys():
        parser.print_help()

    elif args.subcommand == 'search':
        _print_rows(search(_parse_metadata(args.metadata), db=args.db, wd=args.wd, timeout=args.timeout), delimiter=args.delimiter)

    elif args.subcommand == 'add':
        filename = add(_parse_metadata(args.metadata), db=args.db, wd=args.wd, filename=args.filename, timeout=args.timeout, ext=args.ext)
        print(filename)

    elif args.subcommand == 'delete':
        rows = delete(_parse_metadata(args.metadata), db=args.db, wd=args.wd, timeout=args.timeout, dryrun=args.dry_run)
        _print_rows(rows, delimiter=args.delimiter)

    elif args.subcommand == 'test':
        import pytest
        pytest.main([os.path.split(__file__)[0]])

    else:
        raise RuntimeError


if __name__ == '__main__':
    main()
