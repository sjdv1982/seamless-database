# seamless-database

`seamless-database` is the checksum-based metadata and caching service for the [Seamless](https://github.com/sjdv1982/seamless) framework. It acts as the distributed computation cache that allows Seamless workflows to avoid recomputing identical transformations, both within a single session and across the entire cluster.

## How it works

Seamless uses content-addressed storage: every piece of data (buffers, code, parameters) is identified by its checksum. When a transformation (computation) is submitted, its inputs are hashed into a transformation checksum. Before executing the computation, Seamless components (such as `seamless-dask`) query the database: *"has this transformation been computed before?"* If a cached result is found, the result checksum is returned immediately, skipping the computation entirely.

The database stores the following kinds of records:

| Table | Purpose |
|-------|---------|
| **Transformation** | Maps a transformation checksum to its result checksum |
| **RevTransformation** | Reverse lookup: finds which transformations produced a given result |
| **BufferInfo** | Stores buffer metadata (length, dtype, encoding, etc.) for a checksum |
| **SyntacticToSemantic** | Maps between syntactic and semantic checksums per celltype |
| **Expression** | Caches expression evaluation results (input checksum + path + celltype → result checksum) |
| **MetaData** | Stores execution metadata for transformations (executor, environment, timing) |
| **IrreproducibleTransformation** | Records transformations whose results are not reproducible |

All data is persisted in a single SQLite file (typically `seamless.db`).

## Role in the Seamless ecosystem

Other Seamless components interact with the database over HTTP:

- **seamless-dask** checks the database cache before scheduling a transformation on the Dask cluster, and writes results back after computation.
- **seamless-remote** provides the `DatabaseClient` / `DatabaseLaunchedClient` classes that other components use to communicate with the database server.
- **seamless-config** defines the launch template for the database server (port range, host, timeout, read/write mode).

The server exposes a JSON-over-HTTP protocol: clients send `{"type": "<record_type>", "checksum": "<hex>", ...}` via GET (read) or PUT (write) requests.

## Installation

```bash
pip install seamless-database
```

## Usage

```bash
# Start a writable database server on a random port
seamless-database seamless.db --port-range 5520 5530 --writable

# Start a read-only server on a fixed port
seamless-database seamless.db --port 5522
```

### CLI options

| Option | Description |
|--------|-------------|
| `database_file` | Path to the SQLite file (created if it doesn't exist and `--writable` is set) |
| `--port PORT` | Fixed network port |
| `--port-range START END` | Pick a random free port from an inclusive range |
| `--host HOST` | Bind address (default: `0.0.0.0`) |
| `--writable` | Allow PUT requests; opens the database in read/write mode |
| `--status-file FILE` | JSON file used to report server status (for process managers) |
| `--timeout SECONDS` | Stop the server after this many seconds of inactivity |

## CLI scripts

Installing `seamless-database` also provides:

- `seamless-database`
