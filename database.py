from aiohttp import web
import asyncio
import contextlib
import json
import os
import random
import signal
import socket
import sys
import time
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


STATUS_FILE_WAIT_TIMEOUT = 20.0
INACTIVITY_CHECK_INTERVAL = 1.0

status_tracker = None


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


def wait_for_status_file(path: str, timeout: float = STATUS_FILE_WAIT_TIMEOUT):
    deadline = time.monotonic() + timeout
    while True:
        try:
            with open(path, "r", encoding="utf-8") as status_stream:
                contents = json.load(status_stream)
                break
        except FileNotFoundError:
            if time.monotonic() >= deadline:
                print(
                    f"Status file '{path}' not found after {int(timeout)} seconds",
                    file=sys.stderr,
                )
                sys.exit(1)
            time.sleep(0.1)
            continue
        except json.JSONDecodeError as exc:
            print(
                f"Status file '{path}' is not valid JSON: {exc}",
                file=sys.stderr,
            )
            sys.exit(1)

    if not isinstance(contents, dict):
        print(
            f"Status file '{path}' must contain a JSON object",
            file=sys.stderr,
        )
        sys.exit(1)

    return contents


class StatusFileTracker:
    def __init__(self, path: str, base_contents: dict, port: int):
        self.path = path
        self._base_contents = dict(base_contents)
        self.port = port
        self.running_written = False

    def _write(self, payload: dict):
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as status_stream:
            json.dump(payload, status_stream)
            status_stream.write("\n")
        os.replace(tmp_path, self.path)

    def write_running(self):
        payload = dict(self._base_contents)
        payload["port"] = self.port
        payload["status"] = "running"
        self._write(payload)
        self._base_contents = payload
        self.running_written = True

    def write_failed(self):
        payload = dict(self._base_contents)
        payload["status"] = "failed"
        self._write(payload)


def raise_startup_error(exc: BaseException):
    if status_tracker and not status_tracker.running_written:
        status_tracker.write_failed()
    raise exc


def pick_random_free_port(host: str, start: int, end: int) -> int:
    if start < 0 or end > 65535:
        raise RuntimeError("--port-range values must be between 0 and 65535")
    if start > end:
        raise RuntimeError("--port-range START must be less than or equal to END")

    span = end - start + 1
    attempted = set()
    while len(attempted) < span:
        port = random.randint(start, end)
        if port in attempted:
            continue
        attempted.add(port)
        try:
            with socket.create_server((host, port), reuse_port=False):
                pass
        except OSError:
            continue
        return port

    raise RuntimeError(f"No free port available in range {start}-{end}")


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

    def __init__(self, host, port, *, timeout_seconds=None, status_tracker=None):
        self.host = host
        self.port = port
        self._timeout_seconds = timeout_seconds
        self._status_tracker = status_tracker
        self._timeout_task = None
        self._last_request = None
        self._runner = None
        self._site = None

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
        self._runner = runner
        self._site = site
        if self._status_tracker and not self._status_tracker.running_written:
            self._status_tracker.write_running()
        if self._timeout_seconds is not None:
            self._last_request = time.monotonic()
            loop = asyncio.get_running_loop()
            self._timeout_task = loop.create_task(self._monitor_inactivity())

    def start(self):
        if self.future is not None:
            return
        coro = self._start()
        self.future = asyncio.ensure_future(coro)

    async def stop(self):
        if self._timeout_task:
            self._timeout_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._timeout_task
            self._timeout_task = None
        if self._site is not None:
            await self._site.stop()
            self._site = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def _monitor_inactivity(self):
        try:
            while True:
                await asyncio.sleep(INACTIVITY_CHECK_INTERVAL)
                if self._last_request is None:
                    continue
                if time.monotonic() - self._last_request >= self._timeout_seconds:
                    loop = asyncio.get_running_loop()
                    loop.call_soon(loop.stop)
                    break
        except asyncio.CancelledError:
            raise

    def _register_activity(self):
        if self._timeout_seconds is not None:
            self._last_request = time.monotonic()

    async def _healthcheck(self, _):
        return web.Response(status=200, body="OK")

    async def _handle_get(self, request):
        try:
            self._register_activity()
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
            self._register_activity()
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
    port_group = p.add_mutually_exclusive_group()
    port_group.add_argument("--port", type=int, help="Network port")
    port_group.add_argument(
        "--port-range",
        type=int,
        nargs=2,
        metavar=("START", "END"),
        help="Inclusive port range to select a random free port from",
    )
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument(
        "--status-file",
        type=str,
        help="JSON file used to report server status",
    )
    p.add_argument(
        "--timeout",
        type=float,
        help="Stop the server after this many seconds of inactivity",
    )
    args = p.parse_args()

    global status_tracker
    database_file = args.database_file
    print("DATABASE FILE", database_file)
    db_init(database_file)

    selected_port = args.port if args.port is not None else 5522
    status_file_path = args.status_file
    status_tracker = None
    if status_file_path:
        status_file_contents = wait_for_status_file(status_file_path)
        status_tracker = StatusFileTracker(
            status_file_path, status_file_contents, args.port
        )

    if args.port_range:
        start, end = args.port_range
        try:
            selected_port = pick_random_free_port(args.host, start, end)
        except BaseException as exc:
            raise_startup_error(exc)
    if status_tracker:
        status_tracker.port = selected_port

    timeout_seconds = args.timeout
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise_startup_error(RuntimeError("--timeout must be a positive number"))

    def raise_system_exit(*args, **kwargs):
        raise SystemExit

    signal.signal(signal.SIGTERM, raise_system_exit)
    signal.signal(signal.SIGHUP, raise_system_exit)
    signal.signal(signal.SIGINT, raise_system_exit)

    database_server = DatabaseServer(
        args.host,
        selected_port,
        timeout_seconds=timeout_seconds,
        status_tracker=status_tracker,
    )
    database_server.start()

    """
    import logging
    logging.basicConfig()
    logging.getLogger("database").setLevel(logging.DEBUG)
    """

    loop = asyncio.get_event_loop()
    try:
        print("Press Ctrl+C to end")
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    except BaseException:
        if status_tracker and not status_tracker.running_written:
            status_tracker.write_failed()
        raise
    finally:
        loop.run_until_complete(database_server.stop())
