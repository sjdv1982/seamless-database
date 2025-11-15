from aiohttp import web
import asyncio, json, socket
import signal
from peewee import DoesNotExist

from database_models import (
    db_init,
    db_atomic,
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
)


# from the Seamless code
def parse_checksum(checksum, as_bytes=False):
    """Parses checksum and returns it as string"""
    if isinstance(checksum, bytes):
        checksum = checksum.hex()
    if isinstance(checksum, str):
        checksum = bytes.fromhex(checksum)

    if isinstance(checksum, bytes):
        assert len(checksum) == 32, len(checksum)
        if as_bytes:
            return checksum
        else:
            return checksum.hex()

    if checksum is None:
        return
    raise TypeError(type(checksum))


# from the Seamless code
class SeamlessBufferInfo:
    __slots__ = (
        "checksum",
        "length",
        "is_utf8",
        "is_json",
        "json_type",
        "is_json_numeric_array",
        "is_json_numeric_scalar",
        "is_numpy",
        "dtype",
        "shape",
        "is_seamless_mixed",
        "str2text",
        "text2str",
        "binary2bytes",
        "bytes2binary",
        "binary2json",
        "json2binary",
    )

    def __init__(self, checksum, params: dict = {}):
        for slot in self.__slots__:
            setattr(self, slot, params.get(slot))
        if isinstance(checksum, str):
            checksum = parse_checksum(checksum)
        self.checksum = checksum

    def __setattr__(self, attr, value):
        if value is not None:
            if attr == "length":
                if not isinstance(value, int):
                    raise TypeError(type(value))
                if not value >= 0:
                    raise ValueError
            if attr.startswith("is_"):
                if not isinstance(value, bool):
                    raise TypeError(type(value))
        if attr.find("2") > -1 and value is not None:
            if isinstance(value, bytes):
                value = value.hex()
        super().__setattr__(attr, value)

    def __setitem__(self, item, value):
        return setattr(self, item, value)

    def __getitem__(self, item):
        return getattr(self, item)

    def update(self, other):
        if not isinstance(other, SeamlessBufferInfo):
            raise TypeError
        for attr in self.__slots__:
            v = getattr(other, attr)
            if v is not None:
                setattr(self, attr, v)

    def get(self, attr, default=None):
        value = getattr(self, attr)
        if value is None:
            return default
        else:
            return value

    def as_dict(self):
        result = {}
        for attr in self.__slots__:
            if attr == "checksum":
                continue
            v = getattr(self, attr)
            if v is not None:
                result[attr] = v
        return result


def err(*args, **kwargs):
    print("ERROR: " + args[0], *args[1:], **kwargs)
    exit(1)


class DatabaseError(Exception):
    pass


def is_port_in_use(address, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((address, port)) == 0


types = (
    "protocol",
    "buffer_info",
    "syntactic_to_semantic",
    "semantic_to_syntactic",
    "compilation",
    "transformation",
    "elision",
    "metadata",
    "expression",
    "structured_cell_join",
    "contest",  # only PUT
    "rev_expression",  # only GET
    "rev_join",  # only GET
    "rev_transformations",  # only GET
)


def format_response(response, *, none_as_404=False):
    status = None
    if response is None:
        if not none_as_404:
            status = 400
            response = "ERROR: No response"
        else:
            status = 404
            response = "ERROR: Unknown key"
    elif isinstance(response, (bool, dict, list)):
        response = json.dumps(response)
    elif not isinstance(response, (str, bytes)):
        status = 400
        print("ERROR: wrong response format")
        print(type(response), response)
        print("/ERROR: wrong response format")
        response = "ERROR: wrong response format"
    return status, response


class DatabaseServer:
    future = None
    PROTOCOL = ("seamless", "database", "1.0")

    def __init__(self, host, port):
        self.host = host
        self.port = port

    async def _start(self):
        if is_port_in_use(self.host, self.port):  # KLUDGE
            print("ERROR: %s port %d already in use" % (self.host, self.port))
            raise Exception

        app = web.Application(client_max_size=10e9)
        app.add_routes(
            [
                web.get("/healthcheck", self._healthcheck),
                web.get("/{tail:.*}", self._handle_get),
                web.put("/{tail:.*}", self._handle_put),
            ]
        )
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()

    def start(self):
        if self.future is not None:
            return
        coro = self._start()
        self.future = asyncio.ensure_future(coro)

    async def _healthcheck(self, _):
        return web.Response(status=200, body="OK")

    async def _handle_get(self, request):
        try:
            # print("NEW GET REQUEST", hex(id(request)))
            data = await request.read()
            # print("NEW GET REQUEST", data)
            status = 200
            type_ = None
            try:
                try:
                    rq = json.loads(data)
                except Exception:
                    raise DatabaseError("Malformed request") from None
                # print("NEW GET REQUEST DATA", rq)
                try:
                    type_ = rq["type"]
                    if type_ not in types:
                        raise KeyError
                    if type_ != "protocol":
                        checksum = rq["checksum"]
                except KeyError:
                    raise DatabaseError("Malformed request") from None

                if type_ == "protocol":
                    response = list(self.PROTOCOL)
                else:
                    try:
                        checksum = parse_checksum(checksum, as_bytes=False)
                    except ValueError:
                        # import traceback; traceback.print_exc()
                        raise DatabaseError("Malformed request") from None
                    response = await self._get(type_, checksum, rq)
            except DatabaseError as exc:
                status = 400
                if exc.args[0] == "Unknown key":
                    status = 404
                response = "ERROR: " + exc.args[0]
            if isinstance(response, web.Response):
                return response
            status2, response = format_response(response, none_as_404=True)
            if status == 200 and status2 is not None:
                status = status2
            ###if status != 200: print(response)
            return web.Response(status=status, body=response)
        finally:
            # print("END GET REQUEST", hex(id(request)))
            pass

    async def _handle_put(self, request):
        try:
            # print("NEW PUT REQUEST", hex(id(request)))
            data = await request.read()
            # print("NEW PUT REQUEST", data)
            status = 200
            try:
                try:
                    rq = json.loads(data)
                except Exception:
                    import traceback

                    traceback.print_exc()
                    # raise DatabaseError("Malformed request") from None
                if not isinstance(rq, dict):
                    # import traceback; traceback.print_exc()
                    raise DatabaseError("Malformed request")

                # print("NEW PUT REQUEST DATA", rq)
                try:
                    type_ = rq["type"]
                    if type_ not in types:
                        raise KeyError
                    checksum = rq["checksum"]
                except KeyError:
                    # import traceback; traceback.print_exc()
                    raise DatabaseError("Malformed request") from None

                try:
                    checksum = parse_checksum(checksum, as_bytes=False)
                except ValueError:
                    # import traceback; traceback.print_exc()
                    raise DatabaseError("Malformed request") from None

                response = await self._put(type_, checksum, rq)
            except DatabaseError as exc:
                status = 400
                response = "ERROR: " + exc.args[0]
            status2, response = format_response(response)
            if status == 200 and status2 is not None:
                status = status2
            # if status != 200: print(response)
            return web.Response(status=status, body=response)
        finally:
            # print("END PUT REQUEST", hex(id(request)))
            pass

    async def _get(self, type_, checksum, request):
        if type_ == "buffer_info":
            try:
                return json.loads(BufferInfo[checksum].buffer_info)
            except DoesNotExist:
                raise DatabaseError("Unknown key") from None

        elif type_ == "semantic_to_syntactic":
            try:
                celltype, subcelltype = request["celltype"], request["subcelltype"]
            except KeyError:
                raise DatabaseError("Malformed semantic-to-syntactic request")
            results = (
                SyntacticToSemantic.select()
                .where(
                    SyntacticToSemantic.semantic == checksum,
                    SyntacticToSemantic.celltype == celltype,
                    SyntacticToSemantic.subcelltype == subcelltype,
                )
                .execute()
            )
            if results:
                return [parse_checksum(result.syntactic) for result in results]
            raise DatabaseError("Unknown key")

        elif type_ == "syntactic_to_semantic":
            try:
                celltype, subcelltype = request["celltype"], request["subcelltype"]
            except KeyError:
                raise DatabaseError("Malformed syntactic-to-semantic request")
            results = (
                SyntacticToSemantic.select()
                .where(
                    SyntacticToSemantic.syntactic == checksum,
                    SyntacticToSemantic.celltype == celltype,
                    SyntacticToSemantic.subcelltype == subcelltype,
                )
                .execute()
            )
            if results:
                return [parse_checksum(result.semantic) for result in results]
            raise DatabaseError("Unknown key")

        elif type_ == "compilation":
            try:
                return parse_checksum(Compilation[checksum].result)
            except DoesNotExist:
                return None  # None is also a valid response

        elif type_ == "transformation":
            try:
                return parse_checksum(Transformation[checksum].result)
            except DoesNotExist:
                return None  # None is also a valid response

        elif type_ == "elision":
            try:
                return parse_checksum(Elision[checksum].result)
            except DoesNotExist:
                return None  # None is also a valid response

        elif type_ == "metadata":
            try:
                return MetaData[checksum].metadata
            except DoesNotExist:
                return None  # None is also a valid response

        elif type_ == "expression":
            try:
                celltype = request["celltype"]
                path = json.dumps(request["path"])
                hash_pattern = json.dumps(request.get("hash_pattern", ""))
                target_celltype = request["target_celltype"]
                target_hash_pattern = json.dumps(request.get("target_hash_pattern", ""))
            except KeyError:
                raise DatabaseError("Malformed expression request")
            result = (
                Expression.select()
                .where(
                    Expression.input_checksum == checksum,
                    Expression.path == path,
                    Expression.celltype == celltype,
                    Expression.hash_pattern == hash_pattern,
                    Expression.target_celltype == target_celltype,
                    Expression.target_hash_pattern == target_hash_pattern,
                )
                .execute()
            )
            if not result:
                return None
            return parse_checksum(result[0].result)

        elif type_ == "rev_expression":
            expressions = (
                Expression.select()
                .where(
                    Expression.result == checksum,
                )
                .execute()
            )
            if not expressions:
                return None
            result = []
            for expression in expressions:
                expr = {
                    "checksum": expression.input_checksum,
                    "path": json.loads(expression.path),
                    "celltype": expression.celltype,
                    "hash_pattern": json.loads(expression.hash_pattern),
                    "target_celltype": expression.target_celltype,
                    "target_hash_pattern": json.loads(expression.target_hash_pattern),
                    "result": checksum,
                }
                result.append(expr)
            return result

        elif type_ == "rev_join":
            joins = (
                StructuredCellJoin.select()
                .where(
                    StructuredCellJoin.result == checksum,
                )
                .execute()
            )
            if not joins:
                return None
            result = [join.checksum for join in joins]
            return result

        elif type_ == "rev_transformations":
            transformations = (
                RevTransformation.select()
                .where(
                    RevTransformation.result == checksum,
                )
                .execute()
            )
            if not transformations:
                return None
            result = [transformation.checksum for transformation in transformations]
            return result

        elif type_ == "structured_cell_join":
            try:
                return parse_checksum(StructuredCellJoin[checksum].result)
            except DoesNotExist:
                return None  # None is also a valid response

        else:
            raise DatabaseError("Unknown request type")

    async def _put(self, type_, checksum, request):

        if type_ == "buffer_info":
            try:
                value = request["value"]
                if not isinstance(value, dict):
                    raise TypeError
                SeamlessBufferInfo(checksum, value)
                try:
                    existing = json.loads(BufferInfo[checksum].buffer_info)
                    existing.update(value)
                    value = existing
                except DoesNotExist:
                    pass
                value = json.dumps(value, sort_keys=True, indent=2)
            except Exception as exc:
                raise DatabaseError("Malformed PUT buffer info request") from None
            BufferInfo.create(checksum=checksum, buffer_info=value)

        elif type_ == "semantic_to_syntactic":
            try:
                value = request["value"]
                assert isinstance(value, list)
            except Exception:
                raise DatabaseError("Malformed PUT semantic-to-syntactic request")
            try:
                celltype, subcelltype = request["celltype"], request["subcelltype"]
            except KeyError:
                raise DatabaseError(
                    "Malformed PUT semantic-to-syntactic request"
                ) from None
            for syntactic_checksum0 in value:
                syntactic_checksum = parse_checksum(syntactic_checksum0, as_bytes=False)
                with db_atomic():
                    SyntacticToSemantic.create(
                        semantic=checksum,
                        celltype=celltype,
                        subcelltype=subcelltype,
                        syntactic=syntactic_checksum,
                    )

        elif type_ == "compilation":
            try:
                value = parse_checksum(request["value"], as_bytes=False)
            except (KeyError, ValueError):
                raise DatabaseError(
                    "Malformed PUT compilation result request: value must be a checksum"
                ) from None
            Compilation.create(checksum=checksum, result=value)

        elif type_ == "transformation":
            try:
                value = parse_checksum(request["value"], as_bytes=False)
            except (KeyError, ValueError):
                raise DatabaseError(
                    "Malformed PUT transformation result request: value must be a checksum"
                ) from None
            Transformation.create(checksum=checksum, result=value)
            RevTransformation.create(checksum=checksum, result=value)

        elif type_ == "elision":
            try:
                value = parse_checksum(request["value"], as_bytes=False)
            except (KeyError, ValueError):
                raise DatabaseError(
                    "Malformed PUT elision result request: value must be a checksum"
                ) from None
            Elision.create(checksum=checksum, result=value)

        elif type_ == "expression":
            try:
                value = parse_checksum(request["value"], as_bytes=False)
                celltype = request["celltype"]
                path = json.dumps(request["path"])
                hash_pattern = json.dumps(request.get("hash_pattern", ""))
                target_celltype = request["target_celltype"]
                target_hash_pattern = json.dumps(request.get("target_hash_pattern", ""))
            except KeyError:
                raise DatabaseError("Malformed expression request")
            try:
                # assert celltype in celltypes TODO? also for target_celltype
                assert len(path) <= 100
                if len(request["path"]):
                    assert celltype in ("mixed", "plain", "binary")
                assert len(celltype) <= 20
                assert len(hash_pattern) <= 20
                assert len(target_celltype) <= 20
                assert len(target_hash_pattern) <= 20
            except AssertionError:
                raise DatabaseError(
                    "Malformed expression request (constraint violation)"
                )
            Expression.create(
                input_checksum=checksum,
                path=path,
                celltype=celltype,
                hash_pattern=hash_pattern,
                target_celltype=target_celltype,
                target_hash_pattern=target_hash_pattern,
                result=value,
            )

        elif type_ == "structured_cell_join":
            try:
                value = parse_checksum(request["value"], as_bytes=False)
            except (KeyError, ValueError):
                raise DatabaseError(
                    "Malformed PUT structured_cell_join request: value must be a checksum"
                ) from None
            StructuredCellJoin.create(checksum=checksum, result=value)

        elif type_ == "metadata":
            try:
                value = request["value"]
                value = json.loads(value)
            except (KeyError, ValueError):
                raise DatabaseError("Malformed PUT metadata request") from None
            MetaData.create(checksum=checksum, metadata=value)

        elif type_ == "contest":
            try:
                result = parse_checksum(request["result"], as_bytes=False)
            except (KeyError, ValueError):
                raise DatabaseError("Malformed 'contest' request") from None
            in_transformations = False
            try:
                tf = Transformation[checksum]
                tf_result = parse_checksum(tf.result, as_bytes=False)
                in_transformations = True
            except DoesNotExist:
                pass
            if in_transformations:
                if tf_result != result:
                    return web.Response(
                        status=404,
                        reason="Transformation does not have the contested result",
                    )
            try:
                metadata = MetaData[checksum].metadata
                in_metadata = True
            except DoesNotExist:
                metadata = ""
                in_metadata = False
            ContestedTransformation.create(
                checksum=checksum, result=result, metadata=metadata
            )
            if in_transformations:
                tf.delete_instance()
            if in_metadata:
                MetaData[checksum].delete_instance()
        else:
            raise DatabaseError("Unknown request type")
        return "OK"


def main():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument(
        "database_file",
        help="""File where the database is stored.
The database contents are stored as a SQLite file.
If it doesn't exist, a new file is created.""",
    )
    p.add_argument("--port", default=5522, type=int)
    p.add_argument("--host", default="0.0.0.0")
    args = p.parse_args()

    database_file = args.database_file
    print("DATABASE FILE", database_file)
    db_init(database_file)

    def raise_system_exit(*args, **kwargs):
        raise SystemExit

    signal.signal(signal.SIGTERM, raise_system_exit)
    signal.signal(signal.SIGHUP, raise_system_exit)
    signal.signal(signal.SIGINT, raise_system_exit)

    database_server = DatabaseServer(args.host, args.port)
    database_server.start()

    """
    import logging
    logging.basicConfig()
    logging.getLogger("database").setLevel(logging.DEBUG)
    """

    try:
        print("Press Ctrl+C to end")
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        pass
