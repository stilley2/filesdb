import pytest
import os
from . import filesdb


def test_duplication(tmpdir):
    db = "files.db"
    filesdb.add_file(dict(field1='one', field2=2, field3=3.0, field4=True, field5=None), db=db, wd=tmpdir)
    with pytest.raises(ValueError):
        filesdb.add_file(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=tmpdir)
    assert len(filesdb.search({}, db=db, wd=tmpdir)) == 1


def test_file_exists(tmpdir):
    db = "files.db"
    with open(os.path.join(tmpdir, "test"), "w"):
        pass
    with pytest.raises(ValueError):
        filesdb.add_file(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, fname="test", wd=tmpdir)
    assert len(filesdb.search({}, db=db, wd=tmpdir)) == 0


def test_multiple_adds(tmpdir):
    db = "files.db"
    filesdb.add_file(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=tmpdir)
    filesdb.add_file(dict(field1="one", field2=2, field3=3.0, field4=False, field5=None), db=db, wd=tmpdir)
    filesdb.add_file(dict(field1="one", field2=2, field3=3.0, field4=False, field5=42), db=db, wd=tmpdir)
    filesdb.add_file(dict(field1="one", field2=2, field3=4.0, field4=False, field5="some string"), db=db, wd=tmpdir)
    assert len(filesdb.search(dict(field1='one'), db=db, wd=tmpdir)) == 4
    assert len(filesdb.search(dict(field2=2), db=db, wd=tmpdir)) == 4
    assert len(filesdb.search(dict(field2=2, field1="one"), db=db, wd=tmpdir)) == 4
    assert len(filesdb.search(dict(field4=True), db=db, wd=tmpdir)) == 1
    assert len(filesdb.search(dict(field4=False), db=db, wd=tmpdir)) == 3
    assert len(filesdb.search(dict(field4=False, field5=None), db=db, wd=tmpdir)) == 1
    assert len(filesdb.search(dict(field5=None), db=db, wd=tmpdir)) == 2
    assert len(filesdb.search(dict(field5=42), db=db, wd=tmpdir)) == 1
    assert len(filesdb.search(dict(field5="some string"), db=db, wd=tmpdir)) == 1


def test_print_csv(tmpdir):
    db = 'files.db'
    filesdb.add_file(dict(field1="one", field2=2, field3=3.0, field4=True, field5=None), db=db, wd=tmpdir)
    filesdb.add_file(dict(field1="one", field2=2, field3=3.0, field4=False, field5=None), db=db, wd=tmpdir)
    filesdb.add_file(dict(field1="one", field2=2, field3=3.0, field4=False, field5=42), db=db, wd=tmpdir)
    filesdb.add_file(dict(field1="one", field2=2, field3=4.0, field4=False, field5="some string"), db=db, wd=tmpdir)
    filesdb.print_csv(db=db, wd=tmpdir)


def test_reserved_names(tmpdir):
    db = 'files.db'
    with pytest.raises(ValueError):
        filesdb.add_file(dict(time='12'), db=db, wd=tmpdir)
    with pytest.raises(ValueError):
        filesdb.add_file(dict(filename='12'), db=db, wd=tmpdir)
    with pytest.raises(ValueError):
        filesdb.add_file(dict(time='12', filename='fname'), db=db, wd=tmpdir)


def test_unsupported_type(tmpdir):
    db = 'files.db'
    with pytest.raises(ValueError):
        filesdb.add_file(dict(hi=tuple([1, 2, 3])), db=db, wd=tmpdir)
