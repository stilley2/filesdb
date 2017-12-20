from collections import OrderedDict
import pytest
import os
import sqlite3
import subprocess

import filesdb


def test_file_exists(tmpdir):
    db = "files.db"
    with open(os.path.join(str(tmpdir), "test"), "w"):
        pass
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, filename="test", wd=str(tmpdir))
    with pytest.raises(sqlite3.IntegrityError):
        filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, filename="test", wd=str(tmpdir))
    assert len(filesdb.search({}, db=db, wd=str(tmpdir))) == 1


def test_duplication(tmpdir):
    db = "files.db"
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir), filename='1')
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir), filename='2')


def test_multiple_adds(tmpdir):
    db = "files.db"
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=False, field5=None), db=db, wd=str(tmpdir))
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=False, field5=42), db=db, wd=str(tmpdir))
    filesdb.add(dict(field1="one", field2=2, field3=4.0, field4=False, field5="some string"), db=db, wd=str(tmpdir))
    with pytest.raises(sqlite3.IntegrityError):
        filesdb.add(dict(field1="one", field2=2, field3=4.0, field4=False, field5="some string"), db=db, wd=str(tmpdir))
    assert len(filesdb.search(dict(field1='one'), db=db, wd=str(tmpdir))) == 4
    assert len(filesdb.search(dict(field2=2), db=db, wd=str(tmpdir))) == 4
    assert len(filesdb.search(dict(field2=2, field1="one"), db=db, wd=str(tmpdir))) == 4
    assert len(filesdb.search(dict(field4=True), db=db, wd=str(tmpdir))) == 1
    assert len(filesdb.search(dict(field4=False), db=db, wd=str(tmpdir))) == 3
    assert len(filesdb.search(dict(field4=False, field5=None), db=db, wd=str(tmpdir))) == 1
    assert len(filesdb.search(dict(field5=None), db=db, wd=str(tmpdir))) == 2
    assert len(filesdb.search(dict(field5=42), db=db, wd=str(tmpdir))) == 1
    assert len(filesdb.search(dict(field5="some string"), db=db, wd=str(tmpdir))) == 1


def test_new_column(tmpdir):
    db = "files.db"
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None, field6='hi'), db=db, wd=str(tmpdir))
    assert len(filesdb.search(dict(field5=None), db=db, wd=str(tmpdir))) == 2
    assert len(filesdb.search(dict(field6=None), db=db, wd=str(tmpdir))) == 1
    assert len(filesdb.search(dict(field6='hi'), db=db, wd=str(tmpdir))) == 1


def test_reserved_names(tmpdir):
    db = 'files.db'
    with pytest.raises(ValueError):
        filesdb.add(dict(time='12'), db=db, wd=str(tmpdir))
    with pytest.raises(ValueError):
        filesdb.add(dict(filename='12'), db=db, wd=str(tmpdir))
    with pytest.raises(ValueError):
        filesdb.add(dict(time='12', filename='fname'), db=db, wd=str(tmpdir))


def test_unsupported_type(tmpdir):
    db = 'files.db'
    with pytest.raises(RuntimeError):
        filesdb.add(dict(hi=tuple([1, 2, 3])), db=db, wd=str(tmpdir))


def test_delete(tmpdir):
    db = 'files.db'
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, filename="test", wd=str(tmpdir))
    filesdb.delete(dict(filename="test"), wd=str(tmpdir))
    assert len(filesdb.search({}, db=db, wd=str(tmpdir))) == 0
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, filename="test", wd=str(tmpdir))
    assert len(filesdb.search({}, db=db, wd=str(tmpdir))) == 1
    filesdb.add(dict(field1="two", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    filesdb.add(dict(field1="two", field2=3, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    assert len(filesdb.search({}, db=db, wd=str(tmpdir))) == 3
    rows = filesdb.delete(dict(field2=2), wd=str(tmpdir))
    assert len(filesdb.search({}, db=db, wd=str(tmpdir))) == 1
    assert len(rows) == 2


def test_delete_explicit_operator(tmpdir):
    db = 'files.db'
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, filename="test", wd=str(tmpdir))
    filesdb.delete(dict(filename="test"), wd=str(tmpdir))
    assert len(filesdb.search({}, db=db, wd=str(tmpdir))) == 0
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, filename="test", wd=str(tmpdir))
    assert len(filesdb.search({}, db=db, wd=str(tmpdir))) == 1
    filesdb.add(dict(field1="two", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    filesdb.add(dict(field1="two", field2=3, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    assert len(filesdb.search({}, db=db, wd=str(tmpdir))) == 3
    rows = filesdb.delete(dict(field2=2), wd=str(tmpdir), comparison_operators=dict(field1='=='))
    assert len(filesdb.search({}, db=db, wd=str(tmpdir))) == 1
    assert len(rows) == 2


def test_delete_explicit_ne(tmpdir):
    db = 'files.db'
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, filename="test", wd=str(tmpdir))
    filesdb.delete(dict(filename="test"), wd=str(tmpdir))
    assert len(filesdb.search({}, db=db, wd=str(tmpdir))) == 0
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, filename="test", wd=str(tmpdir))
    assert len(filesdb.search({}, db=db, wd=str(tmpdir))) == 1
    filesdb.add(dict(field1="two", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    filesdb.add(dict(field1="two", field2=3, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    assert len(filesdb.search({}, db=db, wd=str(tmpdir))) == 3
    rows = filesdb.delete(dict(field2=2), wd=str(tmpdir), comparison_operators=dict(field2='!='))
    assert len(filesdb.search({}, db=db, wd=str(tmpdir))) == 2
    assert len(rows) == 1


def test_explicit_ne_eq(tmpdir):
    db = 'files.db'
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    filesdb.add(dict(field1="one", field2=3, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    filesdb.add(dict(field1="one", field2=2, field3=4.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    assert len(filesdb.search(dict(field3=3.0), db=db, wd=str(tmpdir), comparison_operators=dict(field3='!='))) == 1
    assert len(filesdb.search(dict(field3=3.0), db=db, wd=str(tmpdir), comparison_operators=dict(field3='<>'))) == 1


def test_explicit_e_eq(tmpdir):
    db = 'files.db'
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    filesdb.add(dict(field1="one", field2=3, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    filesdb.add(dict(field1="one", field2=2, field3=4.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    assert len(filesdb.search(dict(field3=4.0), db=db, wd=str(tmpdir), comparison_operators=dict(field3='='))) == 1
    assert len(filesdb.search(dict(field3=4.0), db=db, wd=str(tmpdir), comparison_operators=dict(field3='=='))) == 1


def test_make_expression_vals():
    metadata = OrderedDict(field1="one", field2="2", field3=3, field4=None)
    assert filesdb._make_expression_vals(metadata, comparison_operators=dict())[0] == "field1=? and field2=? and field3=? and field4 is null"
    assert filesdb._make_expression_vals(metadata, comparison_operators=dict(field1='=', field2='=', field3='=', field4='='))[0] == "field1=? and field2=? and field3=? and field4 is null"
    assert filesdb._make_expression_vals(metadata, comparison_operators=dict(field1='==', field2='==', field3='==', field4='=='))[0] == "field1==? and field2==? and field3==? and field4 is null"
    assert filesdb._make_expression_vals(metadata, comparison_operators=dict(field1='!=', field2='!=', field3='!=', field4='!='))[0] == "field1!=? and field2!=? and field3!=? and field4 is not null"
    assert filesdb._make_expression_vals(metadata, comparison_operators=dict(field1='<>', field2='<>', field3='<>', field4='<>'))[0] == "field1<>? and field2<>? and field3<>? and field4 is not null"


def test_search(tmpdir):
    filesdb.add(dict(field1=1, field2=2, field3=3, field4=4, field5=5), wd=tmpdir, filename='1')
    filesdb.add(dict(field1=1, field2=3, field3=3, field4=4, field5=6), wd=tmpdir, filename='2')
    filesdb.add(dict(field1=1, field2=3, field3=3, field4=4, field5=5, field6=6), wd=tmpdir, filename='3')
    rows = filesdb.search(dict(field5=5, field2=2), wd=tmpdir)
    assert len(rows) == 1
    assert rows[0]['filename'] == '1'
    rows = filesdb.search(dict(field5=5), wd=tmpdir, comparison_operators=dict(field5='<>'))
    assert len(rows) == 1
    assert rows[0]['filename'] == '2'
    rows = filesdb.search(dict(field5=6), wd=tmpdir, comparison_operators=dict(field5='<>'))
    assert len(rows) == 2
    assert rows[0]['filename'] == '1'
    assert rows[1]['filename'] == '3'
    rows = filesdb.search(dict(field5=6, field2=2), wd=tmpdir, comparison_operators=dict(field5='<>'))
    assert len(rows) == 1
    assert rows[0]['filename'] == '1'
    rows = filesdb.search(dict(field6=None), wd=tmpdir, comparison_operators=dict(field6='<>'))
    assert len(rows) == 1
    assert rows[0]['filename'] == '3'
    rows = filesdb.search(dict(field6=None, field5=6), wd=tmpdir, comparison_operators=dict(field5='<>'))
    assert len(rows) == 1
    assert rows[0]['filename'] == '1'
    filesdb.add(dict(field1=1, field2=2, field3=3, field4=4, field5=5, field6=7), wd=tmpdir, filename='4')
    rows = filesdb.search(dict(field6=None), wd=tmpdir, comparison_operators=dict(field6='<>'))
    assert len(rows) == 2
    assert rows[0]['filename'] == '3'
    assert rows[1]['filename'] == '4'
    rows = filesdb.search(dict(field6=None, field2=3), wd=tmpdir, comparison_operators=dict(field6='<>', field2='!='))
    assert len(rows) == 1
    assert rows[0]['filename'] == '4'


def test_str_numeric_equivalence(tmpdir):
    filesdb.add(dict(field1='1', field2=1), wd=str(tmpdir))
    assert len(filesdb.search(dict(field1=1), wd=str(tmpdir))) == 1
    assert len(filesdb.search(dict(field1=1.0), wd=str(tmpdir))) == 1
    filesdb.add(dict(field1='1.0', field2=2), wd=str(tmpdir))
    assert len(filesdb.search(dict(field1=1), wd=str(tmpdir))) == 2
    assert len(filesdb.search(dict(field1=1.0), wd=str(tmpdir))) == 2
    filesdb.add(dict(field1=1.0, field2=3), wd=str(tmpdir))
    assert len(filesdb.search(dict(field1=1.0), wd=str(tmpdir))) == 3
    assert len(filesdb.search(dict(field1=1), wd=str(tmpdir))) == 3
    filesdb.add(dict(field1=1, field2=4), wd=str(tmpdir))
    assert len(filesdb.search(dict(field1=1), wd=str(tmpdir))) == 4
    assert len(filesdb.search(dict(field1=1.0), wd=str(tmpdir))) == 4
    filesdb.add(dict(field1=1e0, field2=5), wd=str(tmpdir))
    assert len(filesdb.search(dict(field1=1), wd=str(tmpdir))) == 5
    assert len(filesdb.search(dict(field1=1.0), wd=str(tmpdir))) == 5
    filesdb.add(dict(field1="one", field2=6), wd=str(tmpdir))
    assert len(filesdb.search(dict(field1=1), wd=str(tmpdir))) == 5
    assert len(filesdb.search(dict(field1=1.0), wd=str(tmpdir))) == 5
    assert len(filesdb.search(dict(field1="one"), wd=str(tmpdir))) == 1
    assert len(filesdb.search(dict(field1="one"), wd=str(tmpdir))) == 1


def test_hash():
    h1 = filesdb._hash_metadata({'test1': 2, 'hi': True, 'string': 'a'})
    h2 = filesdb._hash_metadata({'test1': 2, 'hi': False, 'string': 'a'})
    h3 = filesdb._hash_metadata({'test1': 2, 'string': 'a', 'hi': False})
    assert h1 != h2
    assert h2 == h3
    h4 = filesdb._hash_metadata({'test1': 2.0, 'hi': False, 'string': 'a'})
    h5 = filesdb._hash_metadata({'test1': '2.0', 'hi': False, 'string': 'a'})
    h6 = filesdb._hash_metadata({'test1': '2', 'hi': False, 'string': 'a'})
    h7 = filesdb._hash_metadata({'test1': '2e0', 'hi': False, 'string': 'a'})
    assert h4 == h3
    assert h5 == h3
    assert h6 == h3
    assert h7 == h3


def test_cmd(tmpdir):
    subprocess.check_call(['filesdb', '--wd={}'.format(str(tmpdir)), 'add', 'field1=one', 'field2=2'])
    assert subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search']).decode().count('\n') == 2
    subprocess.check_call(['filesdb', '--wd={}'.format(str(tmpdir)), 'add', 'field1=one', 'field2=3'])
    assert subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search']).decode().count('\n') == 3
    subprocess.check_call(['filesdb', '--wd={}'.format(str(tmpdir)), 'add', 'field1=two', 'field2=2'])
    assert subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search']).decode().count('\n') == 4
    assert subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search', 'field1=two']).decode().count('\n') == 2
    assert subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search', 'field1=one']).decode().count('\n') == 3
    assert subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search', 'field2=2']).decode().count('\n') == 3
    assert subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search', 'field2=3']).decode().count('\n') == 2
    assert subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'delete', '--dry_run', 'field1=one']).decode().count('\n') == 3
    assert subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'delete', '-n', 'field1=one']).decode().count('\n') == 3
    subprocess.check_call(['filesdb', '--wd={}'.format(str(tmpdir)), 'delete', 'field1=one'])
    assert subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search', 'field1=one']).decode().count('\n') == 0
    assert subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search', 'field2=3']).decode().count('\n') == 0
    assert subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search', 'field2=2']).decode().count('\n') == 2
    assert os.path.splitext(subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'add', '--ext=.txt', 'field2=2']).decode().strip())[1] == '.txt'
    assert subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'add', '--filename=hi.txt', 'field2=2']).decode().strip() == 'hi.txt'
    fname = subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'add', '--prefix=hi', '--suffix=there', '--ext=.hdf5', 'field2=2']).decode().strip()
    assert fname[:2] == 'hi'
    assert fname[-10:] == 'there.hdf5'


def test_search_delete_command(tmpdir):
    subprocess.check_call(['filesdb', '--wd={}'.format(str(tmpdir)), 'add', '--filename=1', 'field1=1', 'field2=2', 'field3=3', 'field4=4', 'field5=5'])
    subprocess.check_call(['filesdb', '--wd={}'.format(str(tmpdir)), 'add', '--filename=2', 'field1=1', 'field2=3', 'field3=3', 'field4=4', 'field5=6'])
    subprocess.check_call(['filesdb', '--wd={}'.format(str(tmpdir)), 'add', '--filename=3', 'field1=1', 'field2=3', 'field3=3', 'field4=4', 'field5=5', 'field6=6'])
    rows = subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search', 'field5=5', 'field2=2']).decode()
    assert rows.count('\n') == 2
    assert rows.split('\n')[1].split()[0] == '1'
    rows = subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search', 'field5<>5']).decode()
    assert rows.count('\n') == 2
    assert rows.split('\n')[1].split()[0] == '2'
    rows = subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search', 'field5!=6']).decode()
    assert rows.count('\n') == 3
    assert rows.split('\n')[1].split()[0] == '1'
    assert rows.split('\n')[2].split()[0] == '3'
    rows = subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search', 'field5!=6', 'field2=2']).decode()
    assert rows.count('\n') == 2
    assert rows.split('\n')[1].split()[0] == '1'
    rows = subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search', 'field6<>None']).decode()
    assert rows.count('\n') == 2
    assert rows.split('\n')[1].split()[0] == '3'
    rows = subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search', 'field6==None', 'field5!=6']).decode()
    assert rows.count('\n') == 2
    assert rows.split('\n')[1].split()[0] == '1'
    subprocess.check_call(['filesdb', '--wd={}'.format(str(tmpdir)), 'add', '--filename=4', 'field1=1', 'field2=2', 'field3=3', 'field4=4', 'field5=5', 'field6=7'])
    rows = subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search', 'field6!=None']).decode()
    assert rows.count('\n') == 3
    assert rows.split('\n')[1].split()[0] == '3'
    assert rows.split('\n')[2].split()[0] == '4'
    rows = subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search', 'field6!=None', 'field2!=3']).decode()
    assert rows.count('\n') == 2
    assert rows.split('\n')[1].split()[0] == '4'
    rows = subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'delete', 'field5=5', 'field2=2', 'field6=None']).decode()
    assert rows.count('\n') == 2
    assert rows.split('\n')[1].split()[0] == '1'
    rows = subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search']).decode()
    assert rows.count('\n') == 4
    assert rows.split('\n')[1].split()[0] == '2'
    assert rows.split('\n')[2].split()[0] == '3'
    assert rows.split('\n')[3].split()[0] == '4'
    rows = subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'delete', 'field6<>None']).decode()
    assert rows.count('\n') == 3
    assert rows.split('\n')[1].split()[0] == '3'
    assert rows.split('\n')[2].split()[0] == '4'
    rows = subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search']).decode()
    assert rows.count('\n') == 2
    assert rows.split('\n')[1].split()[0] == '2'
    rows = subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'delete', 'field1<>None']).decode()
    assert rows.count('\n') == 2
    assert rows.split('\n')[1].split()[0] == '2'
    rows = subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'search']).decode()
    assert rows.count('\n') == 0


def test_add_cmd_fail(tmpdir):
    with pytest.raises(subprocess.SubprocessError):
        subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'add', 'field5!=2'])


def test_parse_metadata():
    metadata, co = filesdb._parse_metadata(['field1=1', 'field2==2', 'field3!=3', 'field4<>4'])
    assert metadata['field1'] == '1'
    assert metadata['field2'] == '2'
    assert metadata['field3'] == '3'
    assert metadata['field4'] == '4'
    assert co['field1'] == '='
    assert co['field2'] == '=='
    assert co['field3'] == '!='
    assert co['field4'] == '<>'
    with pytest.raises(ValueError):
        filesdb._parse_metadata(['field1'])
    with pytest.raises(ValueError):
        filesdb._parse_metadata(['field1=1=3'])


def test_cmd_None(tmpdir):
    subprocess.check_call(['filesdb', '--wd={}'.format(str(tmpdir)), 'add', 'field1=None'])
    assert len(filesdb.search(dict(field1=None), wd=str(tmpdir))) == 1


def test_remove_file(tmpdir):
    fname = filesdb.add(dict(field1='test'), wd=str(tmpdir))
    with open(os.path.join(str(tmpdir), fname), 'w'):
        pass
    filesdb.delete(dict(field1='test'), wd=str(tmpdir))
    assert not os.path.exists(os.path.join(str(tmpdir), fname))


def test_delete_error(tmpdir):
    filesdb.add(dict(field1='test'), wd=str(tmpdir))
    with pytest.raises(ValueError):
        filesdb.delete(dict(), wd=str(tmpdir))


def test_db_dne_error(tmpdir):
    with pytest.raises(FileNotFoundError):
        filesdb.delete(dict(), wd=str(tmpdir))
    with pytest.raises(FileNotFoundError):
        filesdb.search(dict(), wd=str(tmpdir))


def test_filename_kwargs(tmpdir):
    with pytest.raises(ValueError):
        filesdb.add(dict(field1='test'), wd=str(tmpdir), filename='1', ext='hi')
    with pytest.raises(ValueError):
        filesdb.add(dict(field1='test'), wd=str(tmpdir), filename='1', prefix='hi')
    with pytest.raises(ValueError):
        filesdb.add(dict(field1='test'), wd=str(tmpdir), filename='1', suffix='hi')
    out = filesdb.add(dict(field1='test'), wd=str(tmpdir), suffix='hi', prefix='there', ext='.wasd')
    assert out[:5] == 'there'
    assert out[-7:] == 'hi.wasd'


def test_repr_html(tmpdir):
    db = "files.db"
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    out = filesdb.search({}, wd=str(tmpdir))
    out._repr_html_()
    out = filesdb.search(dict(field1="two"), wd=str(tmpdir))
    out._repr_html_()
