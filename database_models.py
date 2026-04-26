from peewee import (
    SqliteDatabase,
    Model,
    CharField,
    TextField,
    FixedCharField,
    CompositeKey,
    IntegrityError,
)
import sqlite3
from playhouse.sqlite_ext import JSONField


def ChecksumField(*args, **kwargs):
    return FixedCharField(max_length=64, *args, **kwargs)


_db = SqliteDatabase(
    None,
    pragmas={
        "cache_size": -1 * 64000,  # 64MB
        "foreign_keys": 1,
        "ignore_check_constraints": 0,
        "synchronous": 0,
    },
)


class BaseModel(Model):
    class Meta:
        database = _db
        legacy_table_names = False

    @classmethod
    def create(cls, **kwargs):
        if cls not in _primary:
            return super().create(**kwargs)
        try:
            return super().create(**kwargs)
        except IntegrityError as exc:
            prim = _primary[cls]
            if prim == "id" and prim not in kwargs:
                raise exc from None
            instance = cls.get(**{prim: kwargs[prim]})
            for k, v in kwargs.items():
                setattr(instance, k, v)
            instance.save()


class Transformation(BaseModel):
    checksum = ChecksumField(primary_key=True)
    result = ChecksumField(index=True, unique=False)


class RevTransformation(BaseModel):
    result = ChecksumField(index=True, unique=False)
    checksum = ChecksumField(unique=False)


class BufferInfo(BaseModel):
    # store SeamlessBufferInfo as JSON
    checksum = ChecksumField(primary_key=True)
    buffer_info = TextField()


class SyntacticToSemantic(BaseModel):
    syntactic = ChecksumField(index=True)
    celltype = TextField()
    subcelltype = TextField()
    semantic = ChecksumField(index=True)

    class Meta:
        database = _db
        legacy_table_names = False
        primary_key = CompositeKey(
            "syntactic",
            "celltype",
            "subcelltype",
            "semantic",
        )

    @classmethod
    def create(cls, **kwargs):
        try:
            return super().create(**kwargs)
        except IntegrityError as exc:
            if exc.args[0].split()[0] != "UNIQUE":
                raise exc from None


class Expression(BaseModel):

    input_checksum = ChecksumField()
    path = CharField(max_length=100)
    celltype = CharField(max_length=20)
    target_celltype = CharField(max_length=20)
    validator = ChecksumField(null=True)
    validator_language = CharField(max_length=20, null=True)
    result = ChecksumField(index=True, unique=False)

    class Meta:
        database = _db
        legacy_table_names = False
        primary_key = CompositeKey(
            "input_checksum",
            "path",
            "celltype",
            "target_celltype",
        )

    @classmethod
    def create(cls, **kwargs):
        try:
            return super().create(**kwargs)
        except IntegrityError:
            kwargs2 = {}
            for k in (
                "input_checksum",
                "path",
                "celltype",
                "target_celltype",
            ):
                kwargs2[k] = kwargs[k]
            instance = cls.get(**kwargs2)
            instance.result = kwargs["result"]
            instance.save()


class MetaData(BaseModel):
    # store meta-data for transformations:
    # - executor name (seamless-internal, SLURM, ...)
    # - Seamless version (including Docker/Singularity/conda version)
    # - exact environment conda packages (as environment checksum)
    # - hardware (GPU, memory)
    # - execution time (also if failed)
    # - last recorded progress (if failed)
    checksum = ChecksumField(primary_key=True)
    result = ChecksumField(index=True, unique=False)
    metadata = JSONField()


class BucketProbe(BaseModel):
    bucket_kind = CharField(max_length=20)
    label = TextField()
    bucket_checksum = ChecksumField(index=True, unique=False)
    captured_at = TextField()
    freshness_tokens = JSONField()

    class Meta:
        database = _db
        legacy_table_names = False
        primary_key = CompositeKey("bucket_kind", "label")


class IrreproducibleTransformation(BaseModel):
    result = ChecksumField(index=True, unique=False)
    checksum = ChecksumField(index=True, unique=False)
    metadata = JSONField()


_model_classes = [
    Transformation,
    RevTransformation,
    BufferInfo,
    SyntacticToSemantic,
    Expression,
    MetaData,
    BucketProbe,
    IrreproducibleTransformation,
]
_primary = {}
for model_class in _model_classes:
    if (
        model_class is Expression
        or model_class is SyntacticToSemantic
        or model_class is RevTransformation
        or model_class is MetaData
        or model_class is BucketProbe
    ):
        continue
    for fieldname, field in model_class._meta.fields.items():
        if field.primary_key:
            _primary[model_class] = fieldname
            break
    else:
        raise Exception


def db_init(
    filename,
    init_parameters: dict = None,
    connection_parameters: dict = None,
    *,
    create_tables: bool = True,
):
    if not _db.is_closed():
        _db.close()
    if init_parameters is None:
        init_parameters = {}
    if connection_parameters is None:
        connection_parameters = {}
    _db.init(filename, **init_parameters)
    _db.connect(**connection_parameters)
    if create_tables:
        _ensure_meta_data_schema()
        _db.create_tables(_model_classes, safe=True)


db_atomic = _db.atomic


def _table_columns(table_name: str) -> list[str]:
    cursor = _db.execute_sql(f"PRAGMA table_info({table_name})")
    rows = cursor.fetchall()
    return [row[1] for row in rows]


def _table_rowcount(table_name: str) -> int:
    cursor = _db.execute_sql(f"SELECT COUNT(*) FROM {table_name}")
    row = cursor.fetchone()
    assert row is not None
    return int(row[0])


def _ensure_meta_data_schema() -> None:
    table_name = MetaData._meta.table_name
    try:
        tables = set(_db.get_tables())
    except sqlite3.OperationalError:
        tables = set()
    if table_name not in tables:
        return

    columns = _table_columns(table_name)
    column_set = set(columns)
    if len(columns) == 3 and column_set == {"checksum", "result", "metadata"}:
        return
    if len(columns) == 2 and column_set == {"checksum", "metadata"}:
        if _table_rowcount(table_name) == 0:
            _db.execute_sql(f"DROP TABLE {table_name}")
            return
        raise RuntimeError(
            "Existing legacy 'meta_data' table is non-empty and cannot be upgraded automatically"
        )

    raise RuntimeError(
        f"Unsupported schema for '{table_name}': columns={columns!r}"
    )
