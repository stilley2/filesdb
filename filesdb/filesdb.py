from __future__ import print_function

__all__ = ['add_file', 'search', 'print_csv']

import argparse
import datetime
import os
import sqlite3
import uuid


# https://stackoverflow.com/questions/305378/list-of-tables-db-schema-dump-etc-using-the-python-sqlite3-api
# https://stackoverflow.com/questions/25387537/sqlite3-operationalerror-near-syntax-error
# https://stackoverflow.com/questions/15647580/does-null-have-a-data-type
# https://stackoverflow.com/questions/7519621/where-is-null-not-working-in-sqlite


RESERVED_KEYS = "filename", "time"


def _gettype(obj):
    if obj is None:
        return "blob"  # data can be of any type
    if type(obj) == float:
        return "real"
    if type(obj) == int:
        return "int"
    if type(obj) == str:
        return "text"
    if type(obj) == bool:
        return "int"
    else:
        raise ValueError("unsupported type {} for objection {}".format(type(obj), obj))


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


def _get_conn(db, wd):
    conn = sqlite3.connect(os.path.join(wd, db))
    conn.row_factory = sqlite3.Row
    return conn


def add_file(metadata, db="files.db", wd='.', fname=None):
    for reserved_key in RESERVED_KEYS:
        if reserved_key in metadata.keys():
            raise ValueError("filename is reserved")
    conn = _get_conn(db, wd)
    with conn:
        if conn.execute("select * from sqlite_master where type = 'table' and name = 'filelist'").fetchone() is None:
            conn.execute("create table filelist (filename text, time timestamp)")
        desc = conn.execute("select * from filelist").description
        columns = [d[0] for d in desc]
        for key in metadata.keys():
            if key not in columns:
                conn.execute("alter table filelist add {} {}".format(key, _gettype(metadata[key])))
        keys, vals = _key_val_list(metadata)
        duplicates = search(metadata, db=db, wd=wd)
        if len(duplicates) > 0:
            raise ValueError("Duplicate")
        if fname is None:
            fname = "{}.hdf5".format(uuid.uuid4())
        else:
            if os.path.exists(os.path.join(wd, fname)):
                raise ValueError('{} already exists'.format(fname))
        conn.execute("insert into filelist (filename, time, " + ', '.join(keys) + ") values (" + ', '.join(["?"] * (len(vals) + 2)) + ")", [fname, datetime.datetime.now()] + vals)
    return fname


def search(metadata, db="files.db", wd='.'):
    if not os.path.exists(os.path.join(wd, db)):
        raise RuntimeError('{} not found in {}'.format(db, wd))
    conn = _get_conn(db, wd)
    if len(metadata) > 0:
        keys, vals, null_keys = _key_val_list(metadata, separate_nulls=True)
        search_strs = []
        if len(keys) > 0:
            search_strs.append(" and ".join(["{}=?".format(key) for key in keys]))
        if len(null_keys) > 0:
            search_strs.append(" and ".join(["{} is null".format(key) for key in null_keys]))
        query_string = "select * from filelist where " + " and ".join(search_strs)
        rows = conn.execute(query_string, vals).fetchall()
    else:
        rows = conn.execute("select * from filelist").fetchall()
    return rows


def print_csv(db='files.db', wd='.'):
    rows = search({}, db=db, wd=wd)
    first = True
    for row in rows:
        if first:
            keys = list(row.keys())
            print(','.join(keys))
            first = False
        print(','.join([str(row[key]) for key in keys]))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', type=str, choices=['print_csv', 'test'])
    parser.add_argument('--db', '--database', type=str, default='files.db')
    parser.add_argument('--wd', '--working_directory', type=str, default='.')
    args = parser.parse_args()

    if args.command == 'print_csv':
        print_csv(db=args.db, wd=args.wd)

    if args.command == 'test':
        import pytest
        pytest.main([os.path.split(__file__)[0]])


if __name__ == '__main__':
    main()
