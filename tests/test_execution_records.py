import asyncio
import sqlite3
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
DATABASE_DIR = ROOT / "seamless-database"
if str(DATABASE_DIR) not in sys.path:
    sys.path.insert(0, str(DATABASE_DIR))

from database import DatabaseServer  # noqa: E402
from database_models import (  # noqa: E402
    BucketProbe,
    MetaData,
    RevTransformation,
    Transformation,
    IrreproducibleTransformation,
    _db,
    db_init,
)


TF_CHECKSUM = "1" * 64
RESULT_CHECKSUM = "2" * 64
BUCKET_CHECKSUM = "3" * 64
BUCKET_CHECKSUM_2 = "4" * 64


def _close_db():
    if not _db.is_closed():
        _db.close()


def _init_db(path: Path):
    _close_db()
    db_init(str(path))


def _record(
    tf_checksum: str = TF_CHECKSUM,
    result_checksum: str = RESULT_CHECKSUM,
    schema_version=1,
):
    return {
        "schema_version": schema_version,
        "checksum_fields": ["node", "environment"],
        "tf_checksum": tf_checksum,
        "result_checksum": result_checksum,
    }


def _minimal_record(
    tf_checksum: str = TF_CHECKSUM,
    result_checksum: str = RESULT_CHECKSUM,
):
    return {
        "schema_version": 1,
        "tf_checksum": tf_checksum,
        "result_checksum": result_checksum,
        "seamless_version": "test",
        "execution_mode": "process",
        "remote_target": None,
        "wall_time_seconds": 0.5,
        "cpu_time_user_seconds": 0.2,
        "cpu_time_system_seconds": 0.1,
        "memory_peak_bytes": 12345,
        "gpu_memory_peak_bytes": None,
    }


def test_db_init_recreates_empty_legacy_metadata_table(tmp_path):
    dbfile = tmp_path / "legacy-empty.db"
    con = sqlite3.connect(dbfile)
    try:
        con.execute("CREATE TABLE meta_data (checksum TEXT PRIMARY KEY, metadata TEXT)")
        con.commit()
    finally:
        con.close()

    _init_db(dbfile)
    try:
        con = sqlite3.connect(dbfile)
        try:
            columns = [
                row[1]
                for row in con.execute("PRAGMA table_info(meta_data)").fetchall()
            ]
        finally:
            con.close()
        assert columns == ["checksum", "result", "metadata"]
    finally:
        _close_db()


def test_db_init_rejects_nonempty_legacy_metadata_table(tmp_path):
    dbfile = tmp_path / "legacy-nonempty.db"
    con = sqlite3.connect(dbfile)
    try:
        con.execute("CREATE TABLE meta_data (checksum TEXT PRIMARY KEY, metadata TEXT)")
        con.execute(
            "INSERT INTO meta_data(checksum, metadata) VALUES(?, ?)",
            (TF_CHECKSUM, "{}"),
        )
        con.commit()
    finally:
        con.close()

    with pytest.raises(RuntimeError, match="legacy 'meta_data' table is non-empty"):
        _init_db(dbfile)
    _close_db()


def test_put_metadata_auto_creates_and_gets_record(tmp_path):
    dbfile = tmp_path / "records.db"
    _init_db(dbfile)
    server = DatabaseServer("127.0.0.1", 0)
    record = _record()
    request = {
        "type": "metadata",
        "checksum": TF_CHECKSUM,
        "result": RESULT_CHECKSUM,
        "value": record,
    }

    try:
        result = asyncio.run(server._put("metadata", TF_CHECKSUM, request))
        assert result == "OK"
        assert Transformation[TF_CHECKSUM].result == RESULT_CHECKSUM
        assert MetaData[TF_CHECKSUM].result == RESULT_CHECKSUM
        assert (
            asyncio.run(
                server._get(
                    "metadata", TF_CHECKSUM, {"type": "metadata", "checksum": TF_CHECKSUM}
                )
            )
            == record
        )
    finally:
        _close_db()


def test_put_metadata_accepts_minimal_execution_record(tmp_path):
    dbfile = tmp_path / "minimal-records.db"
    _init_db(dbfile)
    server = DatabaseServer("127.0.0.1", 0)
    record = _minimal_record()
    request = {
        "type": "metadata",
        "checksum": TF_CHECKSUM,
        "result": RESULT_CHECKSUM,
        "value": record,
    }

    try:
        result = asyncio.run(server._put("metadata", TF_CHECKSUM, request))
        assert result == "OK"
        assert MetaData[TF_CHECKSUM].metadata == record
    finally:
        _close_db()


def test_put_metadata_is_idempotent_and_rejects_conflicts(tmp_path):
    dbfile = tmp_path / "records-conflict.db"
    _init_db(dbfile)
    server = DatabaseServer("127.0.0.1", 0)
    record = _record()
    request = {
        "type": "metadata",
        "checksum": TF_CHECKSUM,
        "result": RESULT_CHECKSUM,
        "value": record,
    }
    bad_request = {
        "type": "metadata",
        "checksum": TF_CHECKSUM,
        "result": RESULT_CHECKSUM,
        "value": {
            **record,
            "checksum_fields": ["node"],
        },
    }

    try:
        assert asyncio.run(server._put("metadata", TF_CHECKSUM, request)) == "OK"
        assert asyncio.run(server._put("metadata", TF_CHECKSUM, request)) == "OK"
        response = asyncio.run(server._put("metadata", TF_CHECKSUM, bad_request))
        assert response.status == 409
    finally:
        _close_db()


def test_put_irreproducible_moves_metadata_and_deletes_normal_rows(tmp_path):
    dbfile = tmp_path / "irreproducible.db"
    _init_db(dbfile)
    server = DatabaseServer("127.0.0.1", 0)
    record = _record()

    try:
        asyncio.run(
            server._put(
                "metadata",
                TF_CHECKSUM,
                {
                    "type": "metadata",
                    "checksum": TF_CHECKSUM,
                    "result": RESULT_CHECKSUM,
                    "value": record,
                },
            )
        )
        assert RevTransformation.select().count() == 1
        result = asyncio.run(
            server._put(
                "irreproducible",
                TF_CHECKSUM,
                {
                    "type": "irreproducible",
                    "checksum": TF_CHECKSUM,
                    "result": RESULT_CHECKSUM,
                },
            )
        )
        assert result == "OK"
        assert Transformation.select().count() == 0
        assert MetaData.select().count() == 0
        assert RevTransformation.select().count() == 0
        rows = asyncio.run(
            server._get(
                "irreproducible",
                TF_CHECKSUM,
                {"type": "irreproducible", "checksum": TF_CHECKSUM},
            )
        )
        assert rows == [
            {
                "checksum": TF_CHECKSUM,
                "result": RESULT_CHECKSUM,
                "metadata": record,
            }
        ]
        response = asyncio.run(
            server._put(
                "metadata",
                TF_CHECKSUM,
                {
                    "type": "metadata",
                    "checksum": TF_CHECKSUM,
                    "result": RESULT_CHECKSUM,
                    "value": record,
                },
            )
        )
        assert response.status == 409
        assert IrreproducibleTransformation.select().count() == 1
    finally:
        _close_db()


def test_bucket_probe_roundtrip_and_overwrite(tmp_path):
    dbfile = tmp_path / "bucket-probe.db"
    _init_db(dbfile)
    server = DatabaseServer("127.0.0.1", 0)
    request = {
        "type": "bucket_probe",
        "bucket_kind": "environment",
        "label": "conda:/envs/seamless1",
        "bucket_checksum": BUCKET_CHECKSUM,
        "captured_at": "2026-04-26T12:00:00Z",
        "freshness_tokens": {"conda_meta_mtime": 123},
    }
    expected_probe = {k: v for k, v in request.items() if k != "type"}
    updated_request = {
        **request,
        "bucket_checksum": BUCKET_CHECKSUM_2,
        "captured_at": "2026-04-26T12:05:00Z",
        "freshness_tokens": {"conda_meta_mtime": 456},
    }

    try:
        assert asyncio.run(server._put("bucket_probe", None, request)) == "OK"
        probe = asyncio.run(
            server._get(
                "bucket_probe",
                None,
                {
                    "type": "bucket_probe",
                    "bucket_kind": request["bucket_kind"],
                    "label": request["label"],
                },
            )
        )
        assert probe == expected_probe
        assert BucketProbe.select().count() == 1

        assert asyncio.run(server._put("bucket_probe", None, updated_request)) == "OK"
        probe = asyncio.run(
            server._get(
                "bucket_probe",
                None,
                {
                    "type": "bucket_probe",
                    "bucket_kind": request["bucket_kind"],
                    "label": request["label"],
                },
            )
        )
        assert probe == {k: v for k, v in updated_request.items() if k != "type"}
        assert BucketProbe.select().count() == 1
    finally:
        _close_db()
