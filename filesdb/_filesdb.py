from __future__ import absolute_import
from __future__ import print_function

import datetime
import filecmp
import hashlib
import os
import shutil
import sqlite3


__all__ = ['Row', 'RowList', 'add', 'merge', 'copy', 'delete', 'search', 'search_envs']


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
        conn.execute("create table if not exists filelist (filename text primary key not null, time timestamp, envhash text)")
        conn.execute("create table if not exists environments (envhash text primary key not null)")
        _update_columns(conn, 'filelist', ['envhash'])
    return conn


def _hash_metadata(metadata, envhash=None):
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
    if envhash is not None:
        # only add this if not None to preserve backwards compatibility
        h.update(bytes(str(envhash), 'utf-8'))
    return h.hexdigest()


def _update_columns(conn, table, keys, coltype='NUMERIC'):
    desc = conn.execute("select * from {}".format(table)).description
    columns = [d[0] for d in desc]
    for key in keys:
        if key[-1] == '!':
            raise ValueError('key {} ends in !'.format(key))
        if key not in columns:
            try:
                conn.execute("alter table {} add {} {}".format(table, key, coltype))
            except sqlite3.OperationalError:
                # column already exists. possible due to race condition between populating
                # columns variable and adding the new column
                pass


def add(metadata, db="files.db", wd='.', filename=None, timeout=10, ext='', prefix='', suffix='', copy_mode=False, environment=None):
    if filename and (ext or prefix or suffix):
        raise ValueError('ext, prefix, and suffix cannot be specified if filename is specified')
    if copy_mode:
        metadata = metadata.copy()
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
                raise ValueError("{} is reserved".format(reserved_key))
    if environment is not None and len(environment) > 0:
        hash_ = _add_environment(environment, db=db, wd=wd, timeout=timeout, copy_mode=copy_mode)
    else:
        hash_ = None
    conn = _get_conn(db, wd, timeout=timeout)
    with conn:
        _update_columns(conn, 'filelist', metadata.keys())
        keys, vals = _key_val_list(metadata)
        if filename is None:
            filename = '{}{}{}{}'.format(prefix, _hash_metadata(metadata, envhash=hash_), suffix, ext)
        conn.execute("insert into filelist (filename, time, envhash, " + ', '.join(keys) + ") values (" + ', '.join(["?"] * (len(vals) + 3)) + ")", [filename, currtime, hash_] + vals)
    return filename


def _add_environment(metadata, db="files.db", wd='.', timeout=10, copy_mode=False):

    if copy_mode:
        metadata = metadata.copy()
        if 'envhash' not in metadata:
            raise ValueError('envhash must be in environment in copy_mode')
        hash_ = metadata.pop('envhash')
        if hash_ != _hash_metadata(metadata):
            raise RuntimeError('passed hash does not match calculated. previous hash may be invalid')
    else:
        hash_ = _hash_metadata(metadata)
    conn = _get_conn(db, wd, timeout=timeout)
    existing = len(search_envs({'envhash': hash_}, wd=wd, db=db, timeout=timeout))
    if existing == 0:
        with conn:
            _update_columns(conn, 'environments', metadata.keys())
            keys, vals = _key_val_list(metadata)
            conn.execute("insert into environments (envhash, " + ', '.join(keys) + ") values (" + ', '.join(["?"] * (len(vals) + 1)) + ")", [hash_] + vals)
    return hash_


def _add_many(metadatalist, tablename='filelist', db="files.db", wd='.', timeout=10):
    if len(metadatalist) == 0:
        return
    keys = set()
    for metadata in metadatalist:
        for key in metadata.keys():
            keys.add(key)
    keys = list(keys)
    conn = _get_conn(db, wd, timeout=timeout)
    with conn:
        _update_columns(conn, tablename, keys)
        vals = []
        for metadata in metadatalist:
            tmplist = []
            for key in keys:
                tmplist.append(metadata.get(key, None))
            vals.append(tmplist)
        conn.executemany("insert into {} (".format(tablename) + ', '.join(keys) + ") values (" + ', '.join(["?"] * len(keys)) + ")", vals)


def _parse_key(key):
    if key[-1] == '!':
        key = key[:-1]
        op = '!='
    else:
        op = '='
    return key, op


def _make_expression_vals(metadata, environment=None):
    if environment is None:
        environment = {}
    search_strs = []
    vals_out = []
    for data, table in zip([metadata, environment], ['filelist', 'environments']):
        keys, vals, null_keys = _key_val_list(data, separate_nulls=True)
        vals_out.extend(vals)
        if len(keys) > 0:
            for key in keys:
                key, op = _parse_key(key)
                if op in ['!=', '<>']:
                    search_strs.append('({table}.{key}{op}? or {table}.{key} is null)'.format(table=table, key=key, op=op))
                else:
                    search_strs.append('{table}.{key}{op}?'.format(table=table, key=key, op=op))
        if len(null_keys) > 0:
            nullkey_ops = [_parse_key(key) for key in null_keys]
            search_strs.append(" and ".join(["{table}.{key} {op} null".format(table=table, key=key, op=_NULL_OP_MAP[op]) for key, op in nullkey_ops]))
    expr = " and ".join(search_strs)
    return expr, vals_out


def search(metadata, db="files.db", wd='.', timeout=10, verbose=False, keys_to_print=None, parse_exclamation=False,
           with_environments=False, environment=None):
    if not os.path.exists(os.path.join(wd, db)):
        raise FileNotFoundError('{} does not exist in {}'.format(db, wd))
    conn = _get_conn(db, wd, timeout=timeout)
    basestr = "select * from filelist"
    if with_environments or environment is not None:
        basestr += " inner join environments on filelist.envhash = environments.envhash"
    with conn:
        if len(metadata) > 0 or environment is not None:
            expr, vals = _make_expression_vals(metadata, environment)
            query_string = basestr + " where " + expr
            rows = conn.execute(query_string, vals).fetchall()
        else:
            rows = conn.execute(basestr).fetchall()
    if verbose:
        _print_rows(rows, keys=keys_to_print)
    return RowList(rows)


def search_envs(metadata, db="files.db", wd='.', timeout=10, verbose=False, keys_to_print=None, parse_exclamation=False):
    if not os.path.exists(os.path.join(wd, db)):
        raise FileNotFoundError('{} does not exist in {}'.format(db, wd))
    conn = _get_conn(db, wd, timeout=timeout)
    basestr = "select * from environments"
    with conn:
        if len(metadata) > 0:
            expr, vals = _make_expression_vals({}, metadata)
            query_string = basestr + " where " + expr
            rows = conn.execute(query_string, vals).fetchall()
        else:
            rows = conn.execute(basestr).fetchall()
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


def merge(indb, outdb, wd='.'):
    rowsin = search({}, db=indb, wd=wd)
    envrowsin = search_envs({}, db=indb, wd=wd)
    rowsout = search({}, db=outdb, wd=wd)
    rowsoutdict = {r['filename']: r for r in rowsout}
    fnamesout = {r['filename'] for r in rowsout}
    rowsindict = {r['filename']: r for r in rowsin}
    envrowsindict = {r['envhash']: r for r in envrowsin}
    outenvhashes = {r['envhash'] for r in rowsout}

    metadatalist = []
    newenvhashes = set()
    envdatalist = []
    for fname, row in rowsindict.items():
        if fname in fnamesout:
            if not _cmprows(row, rowsoutdict[fname]):
                raise RuntimeError('{} detected in output database, but with different rows'.format(fname))
        else:
            metadatalist.append(dict(row))
            if row['envhash'] not in outenvhashes and row['envhash'] not in newenvhashes:
                newenvhashes.add(row['envhash'])
                envdatalist.append(dict(envrowsindict[row['envhash']]))
    _add_many(metadatalist, db=outdb, wd=wd)
    _add_many(envdatalist, db=outdb, wd=wd, tablename='environments')


def copy(filename, outdir, db="files.db", wd='.', outdb='files.db', copytype='copy'):
    rowin = search({'filename': filename}, db=db, wd=wd)
    assert len(rowin) == 1
    rowin = rowin[0]
    if rowin['envhash'] is not None:
        envin = search_envs({'envhash': rowin['envhash']}, wd=wd, db=db)
        assert len(envin) == 1
        envin = dict(envin[0])
    else:
        envin = None
    try:
        row = search({'filename': filename}, db=outdb, wd=outdir)
    except FileNotFoundError:
        add(dict(rowin), db=outdb, wd=outdir, copy_mode=True, environment=envin)
    else:
        if len(row) > 1:
            raise RuntimeError('multiple entries found. this should be impossible')
        elif len(row) == 1:
            if not _cmprows(row[0], rowin):
                raise RuntimeError('filename already appears in output database, but with different parameters')
        else:
            add(dict(rowin), db=outdb, wd=outdir, copy_mode=True, environment=envin)
    outfull = os.path.join(outdir, filename)
    infull = os.path.join(wd, filename)
    if os.path.exists(outfull):
        if not filecmp.cmp(outfull, infull, shallow=False):
            raise RuntimeError('File already copied, but results not identical')
    else:
        if copytype == 'hardlink':
            os.link(infull, outfull)
        elif copytype == 'copy':
            shutil.copyfile(infull, outfull)
        else:
            raise ValueError('unsupported copytype')


def delete(metadata, db='files.db', wd='.', timeout=10, dryrun=False, delimiter='\t'):
    if not os.path.exists(os.path.join(wd, db)):
        raise FileNotFoundError('{} does not exist in {}'.format(db, wd))
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
    return RowList(rows)


def _parse_metadata(metadatalist):
    metadata = {}
    for entry in metadatalist:
        split_entry = entry.split('=')
        if len(split_entry) != 2:
            raise ValueError('invalid metadata term: {}'.format(entry))
        key, val = split_entry
        val.strip()
        if val == 'None':
            val = None
        metadata[key.strip()] = val

    return metadata


def _print_rows(rows, delimiter='\t', keys=None):
    first = True
    for row in rows:
        if first:
            if keys is None:
                keys = list(row.keys())
            print(delimiter.join(keys))
            first = False
        print(delimiter.join([str(row[key]) for key in keys]))
