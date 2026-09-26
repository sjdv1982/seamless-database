"""Contract test: contracts/internal/checksum-reference-lifecycle.md §6 *The durable
refholder* and §10.

The durable refholder is conditional: *when* a stored Expression row keeps its
path indirectly (as the checksum of a path buffer), the row is a durable claim
on that buffer. §10 records that the database currently keeps every path inline,
so no indirect row, and therefore no durable claim, arises yet. This test pins
that status: even a very long path is stored inline in the row and round-trips,
and no checksum-shaped indirection appears in its place. When indirect storage
is introduced, this test must be replaced by one for the durable claim itself.
"""

import asyncio
import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
DATABASE_DIR = ROOT / "seamless-database"
if str(DATABASE_DIR) not in sys.path:
    sys.path.insert(0, str(DATABASE_DIR))

from database import DatabaseServer  # noqa: E402
from database_models import Expression as ExpressionRow, _db, db_init  # noqa: E402


INPUT = "a" * 64
RESULT = "b" * 64


def _close_db():
    if not _db.is_closed():
        _db.close()


@pytest.fixture
def server(tmp_path):
    _close_db()
    db_init(str(tmp_path / "contract-reference-lifecycle.db"))
    try:
        yield DatabaseServer("127.0.0.1", 0)
    finally:
        _close_db()


def test_long_expression_path_is_stored_inline(server):
    path = "/".join(f"member{index}" for index in range(20000))  # ~200 kB
    request = {
        "type": "expression",
        "checksum": INPUT,
        "path": path,
        "input_celltype": "plain",
        "celltype": "str",
    }
    response = asyncio.run(server._put("expression", INPUT, {**request, "value": RESULT}))
    assert response == "OK", response

    rows = list(ExpressionRow.select())
    assert len(rows) == 1
    assert json.loads(rows[0].path) == path, "the path is not stored inline"

    assert asyncio.run(server._get("expression", INPUT, request)) == RESULT
