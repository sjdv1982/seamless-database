"""Checksum-only CellJoin records keep their composite identity and reverse index."""
import asyncio
import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from database import DatabaseError, DatabaseServer
import database_models as models

JOIN = 'a' * 64
RESULT = 'b' * 64
OTHER = 'c' * 64

@pytest.fixture
def server(tmp_path):
    if not models._db.is_closed():
        models._db.close()
    models.db_init(str(tmp_path / 'celljoins.db'))
    yield DatabaseServer('127.0.0.1', 0)
    models._db.close()

def put(server, celltype='plain', result=RESULT):
    request = dict(type='celljoin', checksum=JOIN, celltype=celltype, value=result)
    return asyncio.run(server._put('celljoin', JOIN, request))

def get(server, kind, checksum, **extra):
    return asyncio.run(server._get(kind, checksum, dict(type=kind, checksum=checksum, **extra)))

def test_forward_reverse_composite_identity_and_checksum_only_schema(server):
    for celltype in ('mixed', 'plain', 'deepcell', 'deepfolder'):
        assert put(server, celltype) == 'OK'
        assert get(server, 'celljoin', JOIN, celltype=celltype) == RESULT
    rows = get(server, 'rev_celljoins', RESULT)
    assert {(r['checksum'], r['celltype']) for r in rows} == {(JOIN, t) for t in ('mixed', 'plain', 'deepcell', 'deepfolder')}
    assert set(models.CellJoin._meta.fields) == {'checksum', 'celltype', 'result'}
    assert set(models.RevCellJoin._meta.fields) <= {'id', 'checksum', 'celltype', 'result'}
    assert get(server, 'celljoin', OTHER, celltype='plain') is None
    assert get(server, 'rev_celljoins', OTHER) is None

def test_idempotent_put_and_conflict_leave_both_indexes_unchanged(server):
    assert put(server) == 'OK'
    assert put(server) == 'OK'
    assert models.CellJoin.select().count() == 1
    assert models.RevCellJoin.select().count() == 1
    response = put(server, result=OTHER)
    assert response.status == 409
    assert 'CellJoin already exists with different result' in response.text
    assert get(server, 'celljoin', JOIN, celltype='plain') == RESULT
    assert get(server, 'rev_celljoins', OTHER) is None
    assert get(server, 'rev_celljoins', RESULT) == [{'checksum': JOIN, 'celltype': 'plain'}]

@pytest.mark.parametrize('celltype', ['folder', 'bytes', '', None])
def test_invalid_celltype_cannot_create_forward_or_reverse_rows(server, celltype):
    with pytest.raises(DatabaseError):
        put(server, celltype)
    assert models.CellJoin.select().count() == 0
    assert models.RevCellJoin.select().count() == 0

def test_get_requires_celltype(server):
    with pytest.raises(DatabaseError):
        get(server, 'celljoin', JOIN)

def test_reverse_insert_failure_rolls_back_forward_insert(server, monkeypatch):
    import database
    def fail_reverse(*args):
        raise RuntimeError('simulated reverse storage failure')
    monkeypatch.setattr(database, '_ensure_rev_celljoin_row', fail_reverse)
    with pytest.raises(RuntimeError, match='reverse storage failure'):
        put(server)
    assert models.CellJoin.select().count() == 0
    assert models.RevCellJoin.select().count() == 0
