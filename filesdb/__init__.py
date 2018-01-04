from __future__ import absolute_import
from __future__ import print_function

import argparse
import datetime
import filecmp
import hashlib
import os
import sqlite3


# https://stackoverflow.com/questions/305378/list-of-tables-db-schema-dump-etc-using-the-python-sqlite3-api
# https://stackoverflow.com/questions/25387537/sqlite3-operationalerror-near-syntax-error
# https://stackoverflow.com/questions/15647580/does-null-have-a-data-type
# https://stackoverflow.com/questions/7519621/where-is-null-not-working-in-sqlite
# https://stackoverflow.com/questions/16856647/sqlite3-programmingerror-incorrect-number-of-bindings-supplied-the-current-sta
# https://stackoverflow.com/questions/1535327/how-to-print-a-class-or-objects-of-class-using-print


RESERVED_KEYS = "filename", "time"
_NULL_OP_MAP = {'=': 'is', '==': 'is', '!=': 'is not', '<>': 'is not'}


class Row(sqlite3.Row):

    def __str__(self):
        return '\n'.join(['{}: {}'.format(key, self[key]) for key in self.keys()])


class RowList(list):

    def _repr_html_(self):
        if len(self) == 0:
            return
        out = '<table>\n<thead><tr>\n'
        keys = self[0].keys()
        for k in keys:
            out += '<th>{}</th>\n'.format(k)
        out += '</tr>\n</thead>\n<tbody>\n'
        for r in self:
            out += '<tr>\n'
            for k in keys:
                out += '<th>{}</th>\n'.format(r[k])
            out += '</tr>\n'
        out += '</tbody>\n</table>'
        return out


def _key_val_list(d, separate_nulls=False):
    keys = []
    vals = []
    null_list = []
    for key, val in d.items():
        if not isinstance(val, (type(None), int, float, str, bytes)):
            raise RuntimeError('{} has type {}, which is not supported'.format(key, type(val)))
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
    conn.row_factory = Row
    with conn:
        conn.execute("create table if not exists filelist (filename text primary key not null, time timestamp)")
    return conn


def _hash_metadata(metadata):
    keys = list(metadata.keys())
    keys.sort()
    h = hashlib.sha256()
    for k in keys:
        h.update(bytes(str(k), 'utf-8'))
        val = metadata[k]
        if isinstance(val, str):
            try:
                val = float(val)
            except ValueError:
                pass
        elif isinstance(val, int):
            val = float(val)
        h.update(bytes(str(val), 'utf-8'))
    return h.hexdigest()


def add(metadata, db="files.db", wd='.', filename=None, timeout=10, ext='', prefix='', suffix='', copy_mode=False):
    if filename and (ext or prefix or suffix):
        raise ValueError('ext, prefix, and suffix cannot be specified if filename is specified')
    if copy_mode:
        if 'filename' not in metadata:
            raise ValueError('filename must be in metadata in copy_mode')
        if filename is not None:
            raise ValueError('filename kwarg argument must be None in copy_mode')
        filename = metadata.pop('filename')
        if 'time' not in metadata:
            raise ValueError('time must be in metadata in copy_mode')
        currtime = metadata.pop('time')
    else:
        currtime = datetime.datetime.now()
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
            filename = '{}{}{}{}'.format(prefix, _hash_metadata(metadata), suffix, ext)
        conn.execute("insert into filelist (filename, time, " + ', '.join(keys) + ") values (" + ', '.join(["?"] * (len(vals) + 2)) + ")", [filename, currtime] + vals)
    return filename


def _make_expression_vals(metadata, comparison_operators):
    keys, vals, null_keys = _key_val_list(metadata, separate_nulls=True)
    search_strs = []
    if len(keys) > 0:
        search_strs.append(" and ".join(["{}{}?".format(key, comparison_operators.get(key, '=')) for key in keys]))
    if len(null_keys) > 0:
        search_strs.append(" and ".join(["{} {} null".format(key, _NULL_OP_MAP[comparison_operators.get(key, '=')]) for key in null_keys]))
    expr = " and ".join(search_strs)
    return expr, vals


def search(metadata, db="files.db", wd='.', timeout=10, verbose=False, keys_to_print=None, comparison_operators=None):
    if comparison_operators is None:
        comparison_operators = {}
    if not os.path.exists(os.path.join(wd, db)):
        raise FileNotFoundError('{} does not exist in {}'.format(db, wd))
    conn = _get_conn(db, wd, timeout=timeout)
    with conn:
        if len(metadata) > 0:
            expr, vals = _make_expression_vals(metadata, comparison_operators)
            query_string = "select * from filelist where " + expr
            rows = conn.execute(query_string, vals).fetchall()
        else:
            rows = conn.execute("select * from filelist").fetchall()
    if verbose:
        _print_rows(rows, keys=keys_to_print)
    return RowList(rows)


def _cmprows(r1, r2):
    r1 = dict(r1)
    r2 = dict(r2)
    for k in (set(r1.keys()) | set(r2.keys())):
        if r1.get(k) != r2.get(k):
            return False
    return True


def copy(filename, outdir, db="files.db", wd='.', outdb='files.db', copytype='hardlink'):
    rowin = search({'filename': filename}, db=db, wd=wd)
    assert len(rowin) == 1
    rowin = rowin[0]
    try:
        row = search({'filename': filename}, db=outdb, wd=outdir)
    except FileNotFoundError:
        add(dict(rowin), db=outdb, wd=outdir, copy_mode=True)
    else:
        if len(row) > 1:
            raise RuntimeError('multiple entries found. this should be impossible')
        elif len(row) == 1:
            if not _cmprows(row[0], rowin):
                raise RuntimeError('filename already appears in output database, but with different parameters')
        else:
            add(dict(rowin), db=outdb, wd=outdir, copy_mode=True)
    outfull = os.path.join(outdir, filename)
    infull = os.path.join(wd, filename)
    if os.path.exists(outfull):
        if not filecmp.cmp(outfull, infull, shallow=False):
            raise RuntimeError('File already copied, but results not identical')
    else:
        if copytype == 'hardlink':
            os.link(infull, outfull)
        else:
            raise ValueError('unsupported copytype')


def delete(metadata, db='files.db', wd='.', timeout=10, dryrun=False, delimiter='\t', comparison_operators=None):
    if not os.path.exists(os.path.join(wd, db)):
        raise FileNotFoundError('{} does not exist in {}'.format(db, wd))
    if len(metadata) == 0:
        raise ValueError("must have at least one search parameter")
    conn = _get_conn(db, wd, timeout=timeout)
    with conn:
        rows = search(metadata, db=db, wd=wd, timeout=timeout, comparison_operators=comparison_operators)
        if len(rows) > 0:
            if not dryrun:
                for r in rows:
                    if os.path.exists(os.path.join(wd, r['filename'])):
                        os.remove(os.path.join(wd, r['filename']))
                    # potential race condition here, but delete should not be called often
                    # and it should be called directly (not in a script) so the user can
                    # keep track of potential issues.
                    conn.execute('delete from filelist where filename=?', (r['filename'],))
    return RowList(rows)


def _parse_metadata(metadatalist):
    metadata = {}
    comparison_operators = {}
    for entry in metadatalist:
        for operator in ['==', '!=', '=', '<>']:
            split_entry = entry.split(operator)
            if len(split_entry) not in [1, 2]:
                raise ValueError('invalid metadata term: {}'.format(entry))
            if len(split_entry) == 2:
                key, val = split_entry
                val.strip()
                if val == 'None':
                    val = None
                metadata[key.strip()] = val
                comparison_operators[key.strip()] = operator
                break
        else:
            raise ValueError('no suitable operator found in metadata term: {}'.format(entry))

    return metadata, comparison_operators


def _print_rows(rows, delimiter='\t', keys=None):
    first = True
    for row in rows:
        if first:
            if keys is None:
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

    parser_test = subparsers.add_parser('test', help='Run tests')
    parser_test.set_defaults(subcommand='test')

    args = parser.parse_args()

    if 'subcommand' not in vars(args).keys():
        parser.print_help()

    elif args.subcommand == 'search':
        metadata, comparison_operators = _parse_metadata(args.metadata)
        _print_rows(search(metadata, db=args.db, wd=args.wd, timeout=args.timeout, comparison_operators=comparison_operators), delimiter=args.delimiter,
                    keys=None if args.output_columns is None else args.output_columns.split(','))

    elif args.subcommand == 'add':
        metadata, comparison_operators = _parse_metadata(args.metadata)
        for operator in comparison_operators.values():
            if operator not in ['=', '==']:
                raise ValueError('only equality operators allowed for add')
        filename = add(metadata, db=args.db, wd=args.wd, filename=args.filename, timeout=args.timeout, ext=args.ext,
                       prefix=args.prefix, suffix=args.suffix)
        print(filename)

    elif args.subcommand == 'delete':
        metadata, comparison_operators = _parse_metadata(args.metadata)
        rows = delete(metadata, db=args.db, wd=args.wd, timeout=args.timeout, dryrun=args.dry_run, comparison_operators=comparison_operators)
        _print_rows(rows, delimiter=args.delimiter,
                    keys=None if args.output_columns is None else args.output_columns.split(','))

    elif args.subcommand == 'test':
        import pytest
        pytest.main([os.path.split(__file__)[0]])

    else:
        raise RuntimeError


if __name__ == '__main__':
    main()
