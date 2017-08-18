import pytest
import os
import sqlite3
import subprocess

import filesdb


def test_file_exists(tmpdir):
    db = "files.db"
    with open(os.path.join(tmpdir, "test"), "w"):
        pass
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, filename="test", wd=tmpdir)
    with pytest.raises(sqlite3.IntegrityError):
        filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, filename="test", wd=tmpdir)
    assert len(filesdb.search({}, db=db, wd=tmpdir)) == 1


def test_duplication(tmpdir):
    db = "files.db"
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=tmpdir)
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=tmpdir)


def test_multiple_adds(tmpdir):
    db = "files.db"
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=tmpdir)
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=False, field5=None), db=db, wd=tmpdir)
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=False, field5=42), db=db, wd=tmpdir)
    filesdb.add(dict(field1="one", field2=2, field3=4.0, field4=False, field5="some string"), db=db, wd=tmpdir)
    assert len(filesdb.search(dict(field1='one'), db=db, wd=tmpdir)) == 4
    assert len(filesdb.search(dict(field2=2), db=db, wd=tmpdir)) == 4
    assert len(filesdb.search(dict(field2=2, field1="one"), db=db, wd=tmpdir)) == 4
    assert len(filesdb.search(dict(field4=True), db=db, wd=tmpdir)) == 1
    assert len(filesdb.search(dict(field4=False), db=db, wd=tmpdir)) == 3
    assert len(filesdb.search(dict(field4=False, field5=None), db=db, wd=tmpdir)) == 1
    assert len(filesdb.search(dict(field5=None), db=db, wd=tmpdir)) == 2
    assert len(filesdb.search(dict(field5=42), db=db, wd=tmpdir)) == 1
    assert len(filesdb.search(dict(field5="some string"), db=db, wd=tmpdir)) == 1


def test_new_column(tmpdir):
    db = "files.db"
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=tmpdir)
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None, field6='hi'), db=db, wd=tmpdir)
    assert len(filesdb.search(dict(field5=None), db=db, wd=tmpdir)) == 2
    assert len(filesdb.search(dict(field6=None), db=db, wd=tmpdir)) == 1
    assert len(filesdb.search(dict(field6='hi'), db=db, wd=tmpdir)) == 1


def test_reserved_names(tmpdir):
    db = 'files.db'
    with pytest.raises(ValueError):
        filesdb.add(dict(time='12'), db=db, wd=tmpdir)
    with pytest.raises(ValueError):
        filesdb.add(dict(filename='12'), db=db, wd=tmpdir)
    with pytest.raises(ValueError):
        filesdb.add(dict(time='12', filename='fname'), db=db, wd=tmpdir)


def test_unsupported_type(tmpdir):
    db = 'files.db'
    with pytest.raises(sqlite3.InterfaceError):
        filesdb.add(dict(hi=tuple([1, 2, 3])), db=db, wd=tmpdir)


def test_delete(tmpdir):
    db = 'files.db'
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, filename="test", wd=tmpdir)
    filesdb.delete(dict(filename="test"), wd=tmpdir)
    assert len(filesdb.search({}, db=db, wd=tmpdir)) == 0
    filesdb.add(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, filename="test", wd=tmpdir)
    assert len(filesdb.search({}, db=db, wd=tmpdir)) == 1
    filesdb.add(dict(field1="two", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=tmpdir)
    filesdb.add(dict(field1="two", field2=3, field3=3.0, field4=True, field5=None), db=db, wd=tmpdir)
    assert len(filesdb.search({}, db=db, wd=tmpdir)) == 3
    rows = filesdb.delete(dict(field2=2), wd=tmpdir)
    assert len(filesdb.search({}, db=db, wd=tmpdir)) == 1
    assert len(rows) == 2


def test_str_numeric_equivalence(tmpdir):
    filesdb.add(dict(field1='1'), wd=tmpdir)
    assert len(filesdb.search(dict(field1=1), wd=tmpdir)) == 1
    assert len(filesdb.search(dict(field1=1.0), wd=tmpdir)) == 1
    filesdb.add(dict(field1='1.0'), wd=tmpdir)
    assert len(filesdb.search(dict(field1=1), wd=tmpdir)) == 2
    assert len(filesdb.search(dict(field1=1.0), wd=tmpdir)) == 2
    filesdb.add(dict(field1=1.0), wd=tmpdir)
    assert len(filesdb.search(dict(field1=1.0), wd=tmpdir)) == 3
    assert len(filesdb.search(dict(field1=1), wd=tmpdir)) == 3
    filesdb.add(dict(field1=1), wd=tmpdir)
    assert len(filesdb.search(dict(field1=1), wd=tmpdir)) == 4
    assert len(filesdb.search(dict(field1=1.0), wd=tmpdir)) == 4
    filesdb.add(dict(field1="one"), wd=tmpdir)
    assert len(filesdb.search(dict(field1=1), wd=tmpdir)) == 4
    assert len(filesdb.search(dict(field1=1.0), wd=tmpdir)) == 4
    assert len(filesdb.search(dict(field1="one"), wd=tmpdir)) == 1
    assert len(filesdb.search(dict(field1="one"), wd=tmpdir)) == 1


def test_cmd(tmpdir):
    subprocess.check_call(['filesdb', '--wd={}'.format(tmpdir), 'add', 'field1=one', 'field2=2'])
    assert subprocess.check_output(['filesdb', '--wd={}'.format(tmpdir), 'search']).decode().count('\n') == 2
    subprocess.check_call(['filesdb', '--wd={}'.format(tmpdir), 'add', 'field1=one', 'field2=3'])
    assert subprocess.check_output(['filesdb', '--wd={}'.format(tmpdir), 'search']).decode().count('\n') == 3
    subprocess.check_call(['filesdb', '--wd={}'.format(tmpdir), 'add', 'field1=two', 'field2=2'])
    assert subprocess.check_output(['filesdb', '--wd={}'.format(tmpdir), 'search']).decode().count('\n') == 4
    assert subprocess.check_output(['filesdb', '--wd={}'.format(tmpdir), 'search', 'field1=two']).decode().count('\n') == 2
    assert subprocess.check_output(['filesdb', '--wd={}'.format(tmpdir), 'search', 'field1=one']).decode().count('\n') == 3
    assert subprocess.check_output(['filesdb', '--wd={}'.format(tmpdir), 'search', 'field2=2']).decode().count('\n') == 3
    assert subprocess.check_output(['filesdb', '--wd={}'.format(tmpdir), 'search', 'field2=3']).decode().count('\n') == 2
    assert subprocess.check_output(['filesdb', '--wd={}'.format(tmpdir), 'delete', '--dry_run', 'field1=one']).decode().count('\n') == 3
    assert subprocess.check_output(['filesdb', '--wd={}'.format(tmpdir), 'delete', '-n', 'field1=one']).decode().count('\n') == 3
    subprocess.check_call(['filesdb', '--wd={}'.format(tmpdir), 'delete', 'field1=one'])
    assert subprocess.check_output(['filesdb', '--wd={}'.format(tmpdir), 'search', 'field1=one']).decode().count('\n') == 0
    assert subprocess.check_output(['filesdb', '--wd={}'.format(tmpdir), 'search', 'field2=3']).decode().count('\n') == 0
    assert subprocess.check_output(['filesdb', '--wd={}'.format(tmpdir), 'search', 'field2=2']).decode().count('\n') == 2
    assert os.path.splitext(subprocess.check_output(['filesdb', '--wd={}'.format(tmpdir), 'add', '--ext=txt', 'field2=2']).decode().strip())[1] == '.txt'
    assert subprocess.check_output(['filesdb', '--wd={}'.format(tmpdir), 'add', '--filename=hi.txt', 'field2=2']).decode().strip() == 'hi.txt'


def test_cmd_None(tmpdir):
    subprocess.check_call(['filesdb', '--wd={}'.format(tmpdir), 'add', 'field1=None'])
    assert len(filesdb.search(dict(field1=None), wd=tmpdir)) == 1


def test_remove_file(tmpdir):
    fname = filesdb.add(dict(field1='test'), wd=tmpdir)
    with open(os.path.join(tmpdir, fname), 'w'):
        pass
    filesdb.delete(dict(field1='test'), wd=tmpdir)
    assert not os.path.exists(os.path.join(tmpdir, fname))


def test_delete_error(tmpdir):
    filesdb.add(dict(field1='test'), wd=tmpdir)
    with pytest.raises(ValueError):
        filesdb.delete(dict(), wd=tmpdir)
