"""Contract tests for contracts/hashtype.md, *Storage and tightening* (database side)."""

import asyncio
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
DATABASE_DIR = ROOT / "seamless-database"
if str(DATABASE_DIR) not in sys.path:
    sys.path.insert(0, str(DATABASE_DIR))

from database import DatabaseError, DatabaseServer  # noqa: E402
from database_models import HashType as HashTypeRow, _db, db_init  # noqa: E402
from seamless.checksum.hash_type import (  # noqa: E402
    DType,
    Flag,
    HashType,
    Kind,
    Length,
    Rank,
    is_valid_word,
    pack,
)


CHECKSUM = "e" * 64


def _close_db():
    if not _db.is_closed():
        _db.close()


@pytest.fixture
def server(tmp_path):
    _close_db()
    db_init(str(tmp_path / "contract-hashtype.db"))
    try:
        yield DatabaseServer("127.0.0.1", 0)
    finally:
        _close_db()


def _put(server, word):
    request = {"type": "hash_type", "checksum": CHECKSUM, "value": word}
    return asyncio.run(server._put("hash_type", CHECKSUM, request))


def _get(server):
    request = {"type": "hash_type", "checksum": CHECKSUM}
    return asyncio.run(server._get("hash_type", CHECKSUM, request))


MALFORMED_IN_RANGE = {
    "dtype-on-non-numpy": HashType(Kind.RAW_TEXT, Length.SHORT, DType.NUMERIC).word,
    "numpy-without-dtype": HashType(Kind.NUMPY, Length.SHORT, DType.NA).word,
    "rank-on-non-numpy": HashType(Kind.JSON_ARRAY, Length.SHORT, rank=Rank.D1).word,
    "numpy-bytes-on-d1": HashType(
        Kind.NUMPY, Length.SHORT, DType.NONNUMERIC, Rank.D1, Flag.NUMPY_BYTES
    ).word,
    "numpy-bytes-on-json": HashType(Kind.JSON_STRING, Length.SHORT, flags=Flag.NUMPY_BYTES).word,
    "json-number-unflagged": HashType(Kind.JSON_NUMBER, Length.SHORT).word,
    "numeric-scalar-on-object": HashType(
        Kind.JSON_OBJECT, Length.SHORT, flags=Flag.NUMERIC_SCALAR
    ).word,
    "semantic-bit": HashType(Kind.RAW_TEXT, Length.SHORT, flags=Flag.SEMANTIC).word,
    "flags-on-untested": HashType(Kind.UNTESTED, Length.SHORT, flags=Flag.NUMERIC_SCALAR).word,
    "unused-kind-12": 12,
    "bool-true": True,
}


@pytest.mark.parametrize("word", MALFORMED_IN_RANGE.values(), ids=MALFORMED_IN_RANGE.keys())
def test_put_rejects_every_well_formedness_violation(server, word):
    """§The word: well-formedness is enforced by the database (seamless-core is_valid_word)."""
    assert not is_valid_word(word)
    with pytest.raises(DatabaseError, match="Malformed PUT hash_type request"):
        _put(server, word)
    assert HashTypeRow.select().count() == 0


def test_get_of_an_unknown_checksum_is_null(server):
    """§Storage: GET of type hash_type returns the word or null."""
    assert _get(server) is None


@pytest.mark.parametrize(
    "word",
    [
        pack(Kind.JSON_NUMBER, Length.SHORT, flags=Flag.NUMERIC_SCALAR),
        pack(Kind.JSON_STRING, Length.LONG, flags=Flag.NUMERIC_SCALAR),
        pack(Kind.NUMPY, Length.MEDIUM, DType.NONNUMERIC, Rank.SCALAR, Flag.NUMPY_BYTES),
        pack(Kind.NUMPY, Length.LONG, DType.STRUCTURED, Rank.D3PLUS),
        pack(Kind.JSON_UNTESTED, Length.EQ64),
    ],
)
def test_valid_words_round_trip(server, word):
    assert _put(server, word) == "OK"
    assert _get(server) == word


@pytest.mark.parametrize(
    "stored,incoming",
    [
        # UNTESTED is implied by any word of the same length
        (pack(Kind.UNTESTED, Length.MEDIUM), pack(Kind.RAW_BYTES, Length.MEDIUM)),
        (pack(Kind.UNTESTED, Length.MEDIUM), pack(Kind.NUMPY, Length.MEDIUM, DType.NUMERIC)),
        # UTF8_UNTESTED is implied by any is_utf8 word
        (pack(Kind.UTF8_UNTESTED, Length.MEDIUM), pack(Kind.RAW_TEXT, Length.MEDIUM)),
        # JSON_UNTESTED is implied by any is_json word
        (
            pack(Kind.JSON_UNTESTED, Length.MEDIUM),
            pack(Kind.JSON_NUMBER, Length.MEDIUM, flags=Flag.NUMERIC_SCALAR),
        ),
    ],
)
def test_untested_words_tighten_to_every_implying_word(server, stored, incoming):
    """§Storage and tightening: _hash_type_implies, database side."""
    assert _put(server, stored) == "OK"
    assert _put(server, incoming) == "OK"
    assert HashTypeRow[CHECKSUM].hash_type == incoming


@pytest.mark.parametrize(
    "stored,incoming",
    [
        (pack(Kind.UTF8_UNTESTED, Length.MEDIUM), pack(Kind.RAW_BYTES, Length.MEDIUM)),
        (pack(Kind.UTF8_UNTESTED, Length.MEDIUM), pack(Kind.MIXED_OBJECT, Length.MEDIUM)),
        (pack(Kind.JSON_UNTESTED, Length.MEDIUM), pack(Kind.RAW_TEXT, Length.MEDIUM)),
        (pack(Kind.JSON_UNTESTED, Length.MEDIUM), pack(Kind.JSON_UNTESTED, Length.LONG)),
        (pack(Kind.RAW_TEXT, Length.MEDIUM), pack(Kind.JSON_UNTESTED, Length.MEDIUM)),
    ],
)
def test_non_implying_words_conflict(server, stored, incoming):
    """§Storage and tightening: contradictory -> 409; stored word kept."""
    assert _put(server, stored) == "OK"
    response = _put(server, incoming)
    assert response.status == 409
    assert HashTypeRow[CHECKSUM].hash_type == stored


def test_looser_untested_word_is_ignored(server):
    """§Storage and tightening: looser (implied by stored) -> ignored, stored kept."""
    stored = pack(Kind.JSON_UNTESTED, Length.MEDIUM)
    assert _put(server, stored) == "OK"
    assert _put(server, pack(Kind.UTF8_UNTESTED, Length.MEDIUM)) == "OK"
    assert _put(server, pack(Kind.UNTESTED, Length.MEDIUM)) == "OK"
    assert HashTypeRow[CHECKSUM].hash_type == stored
