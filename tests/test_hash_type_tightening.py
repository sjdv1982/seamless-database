import asyncio
import logging
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
DATABASE_DIR = ROOT / "seamless-database"
if str(DATABASE_DIR) not in sys.path:
    sys.path.insert(0, str(DATABASE_DIR))

from database import DatabaseServer  # noqa: E402
from database_models import HashType as HashTypeRow, _db, db_init  # noqa: E402
from seamless import Checksum  # noqa: E402
from seamless.checksum.hash_type import (  # noqa: E402
    Flag,
    Kind,
    Length,
    get_hash_type_cache,
    pack,
    set_hash_type,
)


CHECKSUM = "d" * 64
LADDER = [
    pack(Kind.UNTESTED, Length.SHORT),
    pack(Kind.UTF8_UNTESTED, Length.SHORT),
    pack(Kind.JSON_UNTESTED, Length.SHORT),
    pack(Kind.JSON_STRING, Length.SHORT),
]


def _close_db():
    if not _db.is_closed():
        _db.close()


def _init_db(path: Path):
    _close_db()
    db_init(str(path))


def _request(word):
    return {"type": "hash_type", "checksum": CHECKSUM, "value": word}


def _put(server, word):
    return asyncio.run(server._put("hash_type", CHECKSUM, _request(word)))


@pytest.mark.parametrize("looser,tighter", list(zip(LADDER, LADDER[1:])))
def test_tighter_word_replaces_stored_word(tmp_path, looser, tighter):
    _init_db(tmp_path / "tighter.db")
    server = DatabaseServer("127.0.0.1", 0)
    try:
        assert _put(server, looser) == "OK"
        assert _put(server, tighter) == "OK"
        assert HashTypeRow[CHECKSUM].hash_type == tighter
    finally:
        _close_db()


def test_equal_word_is_accepted_and_changes_nothing(tmp_path):
    _init_db(tmp_path / "equal.db")
    server = DatabaseServer("127.0.0.1", 0)
    word = LADDER[-1]
    try:
        assert _put(server, word) == "OK"
        assert _put(server, word) == "OK"
        assert HashTypeRow.select().count() == 1
        assert HashTypeRow[CHECKSUM].hash_type == word
    finally:
        _close_db()


def test_looser_word_is_accepted_and_keeps_stored_word(tmp_path):
    _init_db(tmp_path / "looser.db")
    server = DatabaseServer("127.0.0.1", 0)
    looser, tighter = LADDER[-2:]
    try:
        assert _put(server, tighter) == "OK"
        assert _put(server, looser) == "OK"
        assert HashTypeRow[CHECKSUM].hash_type == tighter
    finally:
        _close_db()


CONTRADICTIONS = [
    (
        pack(Kind.RAW_TEXT, Length.SHORT),
        pack(Kind.RAW_BYTES, Length.SHORT),
    ),
    (
        pack(Kind.RAW_TEXT, Length.SHORT),
        pack(Kind.RAW_TEXT, Length.MEDIUM),
    ),
    (
        pack(Kind.JSON_STRING, Length.SHORT),
        pack(Kind.JSON_STRING, Length.SHORT, flags=Flag.NUMERIC_SCALAR),
    ),
]


@pytest.mark.parametrize(
    "stored,incoming",
    [pair for pair in CONTRADICTIONS for pair in (pair, pair[::-1])],
)
def test_contradictory_word_conflicts_and_keeps_stored_word(
    tmp_path, caplog, stored, incoming
):
    _init_db(tmp_path / "conflict.db")
    server = DatabaseServer("127.0.0.1", 0)
    try:
        assert _put(server, stored) == "OK"
        with caplog.at_level(logging.ERROR):
            response = _put(server, incoming)
        assert response.status == 409
        assert HashTypeRow[CHECKSUM].hash_type == stored
        assert "Conflicting HashType" in caplog.text
    finally:
        _close_db()


@pytest.mark.parametrize(
    "stored,incoming,outcome",
    [
        (LADDER[1], LADDER[1], "keep"),
        (LADDER[1], LADDER[2], "replace"),
        (LADDER[2], LADDER[1], "keep"),
        (CONTRADICTIONS[0][0], CONTRADICTIONS[0][1], "conflict"),
    ],
)
def test_server_and_local_cache_agree(tmp_path, stored, incoming, outcome):
    _init_db(tmp_path / "agreement.db")
    server = DatabaseServer("127.0.0.1", 0)
    checksum = Checksum(CHECKSUM)
    cache = get_hash_type_cache()
    cache.clear()
    cache[checksum] = stored
    try:
        assert _put(server, stored) == "OK"
        response = _put(server, incoming)
        if outcome == "conflict":
            assert response.status == 409
            with pytest.raises(ValueError, match="Conflicting HashType"):
                set_hash_type(checksum, incoming)
            expected = stored
        else:
            assert response == "OK"
            set_hash_type(checksum, incoming)
            expected = incoming if outcome == "replace" else stored
        assert HashTypeRow[CHECKSUM].hash_type == expected
        assert cache[checksum] == expected
    finally:
        cache.clear()
        _close_db()
