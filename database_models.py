from peewee import (
    SqliteDatabase,
    Model,
    CharField,
    TextField,
    FixedCharField,
    CompositeKey,
    IntegrityError,
)
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
            instance.update(**kwargs)
            instance.save()


class Transformation(BaseModel):
    checksum = ChecksumField(primary_key=True)
    result = ChecksumField(index=True, unique=False)


class RevTransformation(BaseModel):
    result = ChecksumField(index=True, unique=False)
    checksum = ChecksumField(unique=False)


class Elision(BaseModel):
    checksum = ChecksumField(primary_key=True)
    result = ChecksumField()


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


class Compilation(BaseModel):
    checksum = ChecksumField(primary_key=True)
    result = ChecksumField(index=True)


class Expression(BaseModel):

    input_checksum = ChecksumField()
    path = CharField(max_length=100)
    celltype = CharField(max_length=20)
    hash_pattern = CharField(max_length=20)
    target_celltype = CharField(max_length=20)
    target_hash_pattern = CharField(max_length=20)
    result = ChecksumField(index=True, unique=False)

    class Meta:
        database = _db
        legacy_table_names = False
        primary_key = CompositeKey(
            "input_checksum",
            "path",
            "celltype",
            "hash_pattern",
            "target_celltype",
            "target_hash_pattern",
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
                "hash_pattern",
                "target_celltype",
                "target_hash_pattern",
            ):
                kwargs2[k] = kwargs[k]
            instance = cls.get(**kwargs2)
            instance.result = kwargs["result"]
            instance.save()


class StructuredCellJoin(BaseModel):
    """
    Structured cell join:
    input checksum: checksum of a dict containing:
    - auth: auth checksum
    - inchannels: dict-of-checksums where key=inchannel and value=checksum
    - schema: schema checksum (if not empty)
    - hash_pattern: structured cell hash pattern (if not empty)
    result: value checksum of the structured cell
    """

    checksum = ChecksumField(primary_key=True)
    result = ChecksumField(index=True, unique=False)


class MetaData(BaseModel):
    # store meta-data for transformations:
    # - executor name (seamless-internal, SLURM, ...)
    # - Seamless version (including Docker/Singularity/conda version)
    # - exact environment conda packages (as environment checksum)
    # - hardware (GPU, memory)
    # - execution time (also if failed)
    # - last recorded progress (if failed)
    checksum = ChecksumField(primary_key=True)
    metadata = JSONField()


class ContestedTransformation(BaseModel):
    result = ChecksumField(index=True, unique=False)
    checksum = ChecksumField(index=True, unique=False)
    metadata = JSONField()


_model_classes = [
    Transformation,
    RevTransformation,
    Elision,
    BufferInfo,
    SyntacticToSemantic,
    Compilation,
    Expression,
    StructuredCellJoin,
    MetaData,
    ContestedTransformation,
]
_primary = {}
for model_class in _model_classes:
    if (
        model_class is Expression
        or model_class is SyntacticToSemantic
        or model_class is RevTransformation
    ):
        continue
    for fieldname, field in model_class._meta.fields.items():
        if field.primary_key:
            _primary[model_class] = fieldname
            break
    else:
        raise Exception


def db_init(filename, init_parameters: dict = None, connection_parameters: dict = None):
    if init_parameters is None:
        init_parameters = {}
    if connection_parameters is None:
        connection_parameters = {}
    _db.init(filename, **init_parameters)
    _db.connect(**connection_parameters)
    _db.create_tables(_model_classes, safe=True)


db_atomic = _db.atomic
