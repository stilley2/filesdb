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
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir))


def test_multiple_adds(tmpdir):
    db = "files.db"
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=str(tmpdir))
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=False, field5=None), db=db, wd=str(tmpdir))
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=False, field5=42), db=db, wd=str(tmpdir))
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
    with pytest.raises(sqlite3.InterfaceError):
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


def test_str_numeric_equivalence(tmpdir):
    filesdb.add(dict(field1='1'), wd=str(tmpdir))
    assert len(filesdb.search(dict(field1=1), wd=str(tmpdir))) == 1
    assert len(filesdb.search(dict(field1=1.0), wd=str(tmpdir))) == 1
    filesdb.add(dict(field1='1.0'), wd=str(tmpdir))
    assert len(filesdb.search(dict(field1=1), wd=str(tmpdir))) == 2
    assert len(filesdb.search(dict(field1=1.0), wd=str(tmpdir))) == 2
    filesdb.add(dict(field1=1.0), wd=str(tmpdir))
    assert len(filesdb.search(dict(field1=1.0), wd=str(tmpdir))) == 3
    assert len(filesdb.search(dict(field1=1), wd=str(tmpdir))) == 3
    filesdb.add(dict(field1=1), wd=str(tmpdir))
    assert len(filesdb.search(dict(field1=1), wd=str(tmpdir))) == 4
    assert len(filesdb.search(dict(field1=1.0), wd=str(tmpdir))) == 4
    filesdb.add(dict(field1="one"), wd=str(tmpdir))
    assert len(filesdb.search(dict(field1=1), wd=str(tmpdir))) == 4
    assert len(filesdb.search(dict(field1=1.0), wd=str(tmpdir))) == 4
    assert len(filesdb.search(dict(field1="one"), wd=str(tmpdir))) == 1
    assert len(filesdb.search(dict(field1="one"), wd=str(tmpdir))) == 1


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
    assert os.path.splitext(subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'add', '--ext=txt', 'field2=2']).decode().strip())[1] == '.txt'
    assert subprocess.check_output(['filesdb', '--wd={}'.format(str(tmpdir)), 'add', '--filename=hi.txt', 'field2=2']).decode().strip() == 'hi.txt'


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
