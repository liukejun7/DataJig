from __future__ import annotations

import importlib
import json
import math
import os
import platform
import re
import stat
import sys
import tempfile
from collections.abc import Mapping, Sequence
from contextlib import suppress
from importlib import metadata
from pathlib import Path
from types import ModuleType
from typing import Any, NoReturn, Protocol, cast

import blake3

PROTOCOL = "datajig.transform-provider.v1"
PROTOCOL_VERSION = 1
IMPLEMENTATION = "datajig-duckdb-python"
DUCKDB_VERSION = "1.5.6"
SERIALIZER_VERSION = 1
SOURCE_LOADER_POLICY_VERSION = 1
SQL_POLICY_VERSION = 1
MAX_REQUEST_BYTES = 2 * 1024 * 1024
_ALIAS = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_LIMIT_KEYS = frozenset(
    {
        "inputs",
        "source_bytes",
        "source_rows",
        "sql_bytes",
        "parameters",
        "parameter_bytes",
        "output_rows",
        "output_bytes",
        "output_fields",
        "fetch_batch_rows",
        "duckdb_memory_bytes",
        "provider_stdout_bytes",
        "provider_stderr_bytes",
        "wall_time_seconds",
    }
)
_SOURCE_KEYS = frozenset({"alias", "path", "format", "content_id", "bytes", "rows"})
_EXECUTE_KEYS = frozenset(
    {
        "protocol",
        "protocol_version",
        "correlation_id",
        "operation",
        "expected_provider",
        "sources",
        "sql",
        "parameters",
        "id_field",
        "candidate_path",
        "temp_directory",
        "limits",
        "ast_policy_digest",
    }
)
_PROBE_KEYS = frozenset(
    {"protocol", "protocol_version", "correlation_id", "operation", "limits"}
)
_TYPE_NAMES = {
    "BOOLEAN": "boolean",
    "TINYINT": "integer",
    "SMALLINT": "integer",
    "INTEGER": "integer",
    "BIGINT": "integer",
    "UTINYINT": "unsigned_integer",
    "USMALLINT": "unsigned_integer",
    "UINTEGER": "unsigned_integer",
    "UBIGINT": "unsigned_integer",
    "REAL": "double",
    "FLOAT": "double",
    "DOUBLE": "double",
    "VARCHAR": "string",
}


class _Cursor(Protocol):
    description: Sequence[Sequence[Any]] | None

    def fetchmany(self, size: int) -> Sequence[Sequence[Any]]: ...


class _Connection(_Cursor, Protocol):
    def execute(self, query: str, parameters: Sequence[Any] | None = None) -> _Connection: ...

    def close(self) -> None: ...


class ProviderError(Exception):
    def __init__(self, code: str, message: str, remediation: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.remediation = remediation


def _import_duckdb() -> ModuleType:
    return importlib.import_module("duckdb")


def _implementation_version() -> str:
    try:
        return metadata.version("datajig")
    except metadata.PackageNotFoundError:
        return "0.6.0"


def _probe() -> dict[str, object]:
    duckdb = _import_duckdb()
    observed_version = cast(str, duckdb.__version__)
    if observed_version != DUCKDB_VERSION:
        raise ProviderError(
            "PROVIDER_INCOMPATIBLE",
            "The installed DuckDB version is not supported by this DataJig provider.",
            "Install the tested provider with: pip install 'datajig[duckdb]'",
        )
    identity: dict[str, object] = {
        "protocol": PROTOCOL,
        "protocol_version": PROTOCOL_VERSION,
        "implementation": IMPLEMENTATION,
        "implementation_version": _implementation_version(),
        "duckdb_version": observed_version,
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "serializer_version": SERIALIZER_VERSION,
        "source_loader_policy_version": SOURCE_LOADER_POLICY_VERSION,
        "sql_policy_version": SQL_POLICY_VERSION,
    }
    encoded = json.dumps(identity, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    digest = blake3.blake3(b"datajig-transform-provider-v1\0" + encoded).hexdigest()
    identity["provider_id"] = f"provider_{digest}"
    return identity


def _load_sources(connection: _Connection, request: Mapping[str, object]) -> None:
    raw_sources = request.get("sources")
    if not isinstance(raw_sources, list) or not raw_sources:
        _fail("INVALID_REQUEST", "Transform sources are missing.")
    seen: set[str] = set()
    for raw_source in raw_sources:
        source = _object(raw_source, _SOURCE_KEYS, "source")
        alias = _text(source.get("alias"), "source alias")
        if _ALIAS.fullmatch(alias) is None or alias in seen:
            _fail("INVALID_REQUEST", "Transform source aliases must be valid and unique.")
        seen.add(alias)
        path = Path(_text(source.get("path"), "source path"))
        source_format = _text(source.get("format"), "source format")
        _text(source.get("content_id"), "source content ID")
        expected_bytes = _integer(source.get("bytes"), "source bytes")
        expected_rows = _integer(source.get("rows"), "source rows")
        try:
            source_stat = path.lstat()
        except OSError:
            _fail("SOURCE_UNAVAILABLE", "A staged transform source is unavailable.")
        if (
            not path.is_absolute()
            or stat.S_ISLNK(source_stat.st_mode)
            or not stat.S_ISREG(source_stat.st_mode)
        ):
            _fail("SOURCE_UNAVAILABLE", "A staged transform source is not a regular file.")
        if source_stat.st_size != expected_bytes:
            _fail("SOURCE_DRIFT", "A staged transform source changed before loading.")
        if source_format == "jsonl":
            _validate_flat_jsonl(path, expected_rows)
        identifier = f'"{alias}"'
        try:
            if source_format == "csv":
                connection.execute(
                    f"CREATE TEMP TABLE {identifier} AS SELECT * FROM read_csv(?, "
                    "header=true, all_varchar=true, delim=',', quote='\"', escape='\"', "
                    "strict_mode=true, encoding='utf-8')",
                    [str(path)],
                )
            elif source_format == "parquet":
                connection.execute(
                    f"CREATE TEMP TABLE {identifier} AS SELECT * FROM read_parquet(?)", [str(path)]
                )
            elif source_format == "jsonl":
                connection.execute(
                    f"CREATE TEMP TABLE {identifier} AS SELECT * FROM read_json_auto(?, "
                    "format='newline_delimited', union_by_name=true)",
                    [str(path)],
                )
            else:
                _fail("INVALID_REQUEST", "A transform source format is unsupported.")
            observed_rows = cast(
                int,
                connection.execute(f"SELECT count(*) FROM {identifier}").fetchmany(1)[0][0],
            )
        except ProviderError:
            raise
        except Exception as error:
            raise ProviderError(
                "SOURCE_LOAD_FAILED",
                "DuckDB could not load a staged transform source.",
                "Verify the declared format, encoding, header, and scalar values.",
            ) from error
        if observed_rows != expected_rows:
            _fail("SOURCE_DRIFT", "A staged transform source row count changed before execution.")


def _lockdown(connection: _Connection, sandbox: Path) -> None:
    if sandbox.is_symlink():
        _fail("INVALID_REQUEST", "The provider temp directory is invalid.")
    resolved = sandbox.resolve(strict=True)
    if not resolved.is_dir():
        _fail("INVALID_REQUEST", "The provider temp directory is invalid.")
    settings: list[tuple[str, object]] = [
        ("temp_directory", str(resolved)),
        ("memory_limit", "512MiB"),
        ("threads", 1),
        ("autoinstall_known_extensions", False),
        ("autoload_known_extensions", False),
        ("allow_persistent_secrets", False),
        ("python_enable_replacements", False),
        ("enable_external_access", False),
    ]
    try:
        for name, value in settings:
            connection.execute(f"SET {name} = ?", [value])
        expected: dict[str, object] = {
            "temp_directory": str(resolved),
            "threads": 1,
            "autoinstall_known_extensions": False,
            "autoload_known_extensions": False,
            "allow_persistent_secrets": False,
            "python_enable_replacements": False,
            "enable_external_access": False,
        }
        for name, value in expected.items():
            observed = connection.execute(f"SELECT current_setting('{name}')").fetchmany(1)[0][0]
            if observed != value:
                _fail("PROVIDER_INCOMPATIBLE", "DuckDB did not apply a required lockdown setting.")
        memory = connection.execute("SELECT current_setting('memory_limit')").fetchmany(1)[0][0]
        if memory != "512.0 MiB":
            _fail("PROVIDER_INCOMPATIBLE", "DuckDB did not apply the required memory limit.")
        if connection.execute("SELECT count(*) FROM duckdb_secrets()").fetchmany(1)[0][0] != 0:
            _fail("PROVIDER_INCOMPATIBLE", "DuckDB contains an unexpected secret.")
        databases = connection.execute("PRAGMA database_list").fetchmany(2)
        if len(databases) != 1 or databases[0][1:] != ("memory", None):
            _fail("PROVIDER_INCOMPATIBLE", "DuckDB contains an unexpected attachment.")
        connection.execute("SET lock_configuration = true")
    except ProviderError:
        raise
    except Exception as error:
        raise ProviderError(
            "PROVIDER_INCOMPATIBLE",
            "DuckDB cannot enforce the required query lockdown.",
            "Install the tested provider with: pip install 'datajig[duckdb]'",
        ) from error


def _stream_result(
    cursor: _Cursor,
    candidate_path: Path,
    batch_rows: int,
    *,
    max_rows: int = 2_000_000,
    max_bytes: int = 512 * 1024 * 1024,
    max_fields: int = 256,
) -> dict[str, object]:
    description = cursor.description
    if description is None or not description or len(description) > max_fields:
        _fail("OUTPUT_SCHEMA_INVALID", "The transform output schema is empty or too wide.")
    names: list[str] = []
    wire_types: list[str] = []
    schema: list[dict[str, object]] = []
    for field in description:
        name = field[0]
        if (
            not isinstance(name, str)
            or not name
            or len(name.encode("utf-8")) > 1024
            or name in names
        ):
            _fail(
                "OUTPUT_SCHEMA_INVALID",
                "Transform output field names must be nonempty and unique.",
            )
        duckdb_type = str(field[1]).upper()
        wire_type = _TYPE_NAMES.get(duckdb_type)
        if wire_type is None:
            _fail(
                "OUTPUT_TYPE_UNSUPPORTED",
                f"Transform output field {name!r} has unsupported type {duckdb_type}.",
                "Cast the value explicitly to VARCHAR or a supported scalar type.",
            )
        nullable = not (len(field) > 6 and field[6] is False)
        names.append(name)
        wire_types.append(wire_type)
        schema.append({"name": name, "value_type": wire_type, "nullable": nullable})
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(candidate_path, flags, 0o600)
    rows = 0
    written = 0
    try:
        with os.fdopen(descriptor, "wb") as output:
            while True:
                batch = cursor.fetchmany(batch_rows)
                if not batch:
                    break
                for raw_row in batch:
                    if len(raw_row) != len(names):
                        _fail(
                            "OUTPUT_SCHEMA_INVALID",
                            "A transform output row does not match its schema.",
                        )
                    record = {
                        name: _wire_value(value, wire_type)
                        for name, wire_type, value in zip(names, wire_types, raw_row, strict=True)
                    }
                    encoded = (
                        json.dumps(
                            record,
                            ensure_ascii=False,
                            separators=(",", ":"),
                            allow_nan=False,
                        ).encode("utf-8")
                        + b"\n"
                    )
                    rows += 1
                    written += len(encoded)
                    if rows > max_rows or written > max_bytes:
                        _fail(
                            "OUTPUT_LIMIT_EXCEEDED",
                            "The transform output exceeds its declared limits.",
                        )
                    output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
    except BaseException:
        with suppress(FileNotFoundError):
            candidate_path.unlink()
        raise
    return {"schema": schema, "rows": rows, "bytes": written, "candidate_complete": True}


def _handle_request(raw_request: object) -> dict[str, object]:
    correlation_id = "unknown"
    try:
        request = _object(raw_request, None, "request")
        raw_correlation = request.get("correlation_id")
        if isinstance(raw_correlation, str) and raw_correlation:
            correlation_id = raw_correlation
        _validate_envelope(request)
        operation = request["operation"]
        if operation == "probe":
            _object(request, _PROBE_KEYS, "probe request")
            identity = _probe()
            with tempfile.TemporaryDirectory(prefix="datajig-provider-probe-") as directory:
                duckdb = _import_duckdb()
                connection = cast(_Connection, duckdb.connect(":memory:"))
                try:
                    _lockdown(connection, Path(directory))
                finally:
                    connection.close()
            return {
                "protocol": PROTOCOL,
                "protocol_version": PROTOCOL_VERSION,
                "correlation_id": correlation_id,
                "status": "ok",
                "provider": identity,
                "lockdown_supported": True,
            }
        if operation != "execute":
            _fail("INVALID_REQUEST", "The provider operation is unsupported.")
        request = _object(request, _EXECUTE_KEYS, "execute request")
        identity = _probe()
        if request.get("expected_provider") != identity:
            _fail("PROVIDER_DRIFT", "The running provider identity does not match the request.")
        limits = _validate_limits(request.get("limits"))
        sources = request.get("sources")
        if not isinstance(sources, list) or not 1 <= len(sources) <= limits["inputs"]:
            _fail("INVALID_REQUEST", "The transform source count is invalid.")
        source_descriptors = [_object(source, _SOURCE_KEYS, "source") for source in sources]
        source_bytes = sum(
            _integer(source.get("bytes"), "source bytes") for source in source_descriptors
        )
        source_rows = sum(
            _integer(source.get("rows"), "source rows") for source in source_descriptors
        )
        if source_bytes > limits["source_bytes"] or source_rows > limits["source_rows"]:
            _fail("INVALID_REQUEST", "The transform sources exceed their declared limits.")
        sql = _text(request.get("sql"), "SQL")
        if len(sql.encode("utf-8")) > limits["sql_bytes"]:
            _fail("INVALID_REQUEST", "The transform SQL exceeds its declared limit.")
        parameters = request.get("parameters")
        if not isinstance(parameters, list) or len(parameters) > limits["parameters"]:
            _fail("INVALID_REQUEST", "The transform parameters are invalid.")
        if any(not _is_scalar(value) for value in parameters):
            _fail("INVALID_REQUEST", "Transform parameters must be JSON scalars.")
        parameter_bytes = len(
            json.dumps(parameters, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        )
        if parameter_bytes > limits["parameter_bytes"]:
            _fail("INVALID_REQUEST", "The transform parameters exceed their byte limit.")
        _text(request.get("id_field"), "ID field")
        _text(request.get("ast_policy_digest"), "AST policy digest")
        candidate = Path(_text(request.get("candidate_path"), "candidate path"))
        temp_directory = Path(_text(request.get("temp_directory"), "temp directory"))
        _validate_sandbox_paths(sources, candidate, temp_directory)
        duckdb = _import_duckdb()
        connection = cast(_Connection, duckdb.connect(":memory:"))
        try:
            _load_sources(connection, request)
            _lockdown(connection, temp_directory)
            cursor = connection.execute(sql, parameters)
            result = _stream_result(
                cursor,
                candidate,
                limits["fetch_batch_rows"],
                max_rows=limits["output_rows"],
                max_bytes=limits["output_bytes"],
                max_fields=limits["output_fields"],
            )
        finally:
            connection.close()
        id_field = _text(request.get("id_field"), "ID field")
        output_schema = cast(list[dict[str, object]], result["schema"])
        if id_field not in {field["name"] for field in output_schema}:
            candidate.unlink(missing_ok=True)
            _fail("OUTPUT_SCHEMA_INVALID", "The transform output does not contain the ID field.")
        return {
            "protocol": PROTOCOL,
            "protocol_version": PROTOCOL_VERSION,
            "correlation_id": correlation_id,
            "status": "ok",
            "provider": identity,
            **result,
        }
    except ModuleNotFoundError:
        return _error_response(
            correlation_id,
            ProviderError(
                "PROVIDER_UNAVAILABLE",
                "The optional DuckDB transform provider is not installed.",
                "Install it with: pip install 'datajig[duckdb]'",
            ),
        )
    except ProviderError as error:
        return _error_response(correlation_id, error)
    except Exception:
        return _error_response(
            correlation_id,
            ProviderError(
                "PROVIDER_EXECUTION_FAILED",
                "The DuckDB transform provider failed without publishing an output.",
                "Inspect the transform contract and retry with a new plan.",
            ),
        )


def main() -> int:
    try:
        payload = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
        if len(payload) > MAX_REQUEST_BYTES:
            _fail("INVALID_REQUEST", "The provider request exceeds its protocol bound.")
        request = json.loads(
            payload,
            object_pairs_hook=_strict_object,
            parse_constant=_reject_json_constant,
        )
        response = _handle_request(request)
    except (ProviderError, UnicodeDecodeError, json.JSONDecodeError) as error:
        provider_error = error if isinstance(error, ProviderError) else ProviderError(
            "INVALID_REQUEST",
            "The provider request is not strict UTF-8 JSON.",
            "Regenerate the request with the matching DataJig CLI.",
        )
        response = _error_response("unknown", provider_error)
    sys.stdout.write(json.dumps(response, ensure_ascii=False, separators=(",", ":")) + "\n")
    return 0 if response["status"] == "ok" else 1


def _validate_envelope(request: Mapping[str, object]) -> None:
    if request.get("protocol") != PROTOCOL or request.get("protocol_version") != PROTOCOL_VERSION:
        _fail("PROTOCOL_MISMATCH", "The provider protocol version does not match.")
    _text(request.get("correlation_id"), "correlation ID")
    _validate_limits(request.get("limits"))


def _validate_limits(raw_limits: object) -> dict[str, int]:
    limits = _object(raw_limits, _LIMIT_KEYS, "limits")
    result = {name: _integer(limits.get(name), name) for name in _LIMIT_KEYS}
    maximums = {
        "inputs": 16,
        "source_bytes": 512 * 1024 * 1024,
        "source_rows": 2_000_000,
        "sql_bytes": 65_536,
        "parameters": 256,
        "parameter_bytes": 65_536,
        "output_rows": 2_000_000,
        "output_bytes": 512 * 1024 * 1024,
        "output_fields": 256,
        "fetch_batch_rows": 65_536,
        "duckdb_memory_bytes": 512 * 1024 * 1024,
        "provider_stdout_bytes": 1024 * 1024,
        "provider_stderr_bytes": 1024 * 1024,
        "wall_time_seconds": 900,
    }
    if any(value < 0 or value > maximums[name] for name, value in result.items()):
        _fail("INVALID_REQUEST", "Transform limits exceed provider protocol v1.")
    required_positive = set(_LIMIT_KEYS) - {"parameters"}
    if any(result[name] == 0 for name in required_positive):
        _fail("INVALID_REQUEST", "Transform limits must be positive.")
    return result


def _validate_sandbox_paths(sources: object, candidate: Path, temp_directory: Path) -> None:
    if not isinstance(sources, list) or candidate.exists():
        _fail("INVALID_REQUEST", "The provider sandbox paths are invalid.")
    try:
        sandbox = candidate.parent.resolve(strict=True)
        resolved_temp = temp_directory.resolve(strict=True)
    except OSError:
        _fail("INVALID_REQUEST", "The provider sandbox paths are unavailable.")
    if not candidate.is_absolute() or sandbox != resolved_temp.parent or not resolved_temp.is_dir():
        _fail("INVALID_REQUEST", "The provider output and temp paths must share one sandbox.")
    for raw_source in sources:
        source = _object(raw_source, _SOURCE_KEYS, "source")
        source_path = Path(_text(source.get("path"), "source path"))
        try:
            parent = source_path.parent.resolve(strict=True)
        except OSError:
            _fail("SOURCE_UNAVAILABLE", "A staged transform source is unavailable.")
        if not source_path.is_absolute() or parent != sandbox:
            _fail("INVALID_REQUEST", "Every staged source must remain inside the provider sandbox.")


def _validate_flat_jsonl(path: Path, expected_rows: int) -> None:
    rows = 0
    try:
        with path.open("r", encoding="utf-8", errors="strict", newline="") as stream:
            for line in stream:
                if not line.strip():
                    continue
                value = json.loads(
                    line,
                    object_pairs_hook=_strict_object,
                    parse_constant=_reject_json_constant,
                )
                if not isinstance(value, dict) or any(
                    not _is_scalar(item) for item in value.values()
                ):
                    _fail("SOURCE_LOAD_FAILED", "JSONL sources must contain flat scalar objects.")
                rows += 1
    except ProviderError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ProviderError(
            "SOURCE_LOAD_FAILED",
            "A JSONL source is not valid UTF-8 newline-delimited JSON.",
            "Provide one flat scalar JSON object per nonblank line.",
        ) from error
    if rows != expected_rows:
        _fail("SOURCE_DRIFT", "A staged JSONL source row count changed before execution.")


def _wire_value(value: object, wire_type: str) -> object:
    if value is None:
        return None
    if wire_type == "boolean" and type(value) is bool:
        return value
    if wire_type in {"integer", "unsigned_integer"} and type(value) is int:
        integer = value
        minimum = 0 if wire_type == "unsigned_integer" else -(2**63)
        maximum = 2**64 - 1 if wire_type == "unsigned_integer" else 2**63 - 1
        if minimum <= integer <= maximum:
            return integer
    if wire_type == "double" and type(value) is float:
        number = value
        if math.isfinite(number):
            return 0 if number == 0 else number
    if wire_type == "string" and isinstance(value, str):
        return value
    _fail("OUTPUT_VALUE_INVALID", "A transform output value does not match its declared type.")


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ProviderError(
                "INVALID_REQUEST",
                "Provider JSON contains a duplicate object member.",
                "Regenerate the request with the matching DataJig CLI.",
            )
        result[key] = value
    return result


def _reject_json_constant(value: str) -> NoReturn:
    raise ProviderError(
        "INVALID_REQUEST",
        f"Provider JSON contains non-finite number {value}.",
        "Use only finite JSON numbers.",
    )


def _object(value: object, keys: frozenset[str] | None, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        _fail("INVALID_REQUEST", f"The provider {label} must be an object.")
    result = cast(dict[str, object], value)
    if keys is not None and frozenset(result) != keys:
        _fail("INVALID_REQUEST", f"The provider {label} has missing or unknown fields.")
    return result


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 1024 * 1024:
        _fail("INVALID_REQUEST", f"The provider {label} is invalid.")
    return value


def _integer(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        _fail("INVALID_REQUEST", f"The provider {label} must be a nonnegative integer.")
    return value


def _is_scalar(value: object) -> bool:
    if value is None or type(value) in {bool, str}:
        return True
    if type(value) is int:
        return -(2**63) <= value <= 2**64 - 1
    if type(value) is float:
        return math.isfinite(value)
    return False


def _error_response(correlation_id: str, error: ProviderError) -> dict[str, object]:
    return {
        "protocol": PROTOCOL,
        "protocol_version": PROTOCOL_VERSION,
        "correlation_id": correlation_id,
        "status": "error",
        "error": {
            "code": error.code,
            "message": error.message,
            "remediation": error.remediation,
        },
    }


def _fail(
    code: str,
    message: str,
    remediation: str = "Regenerate the transform request.",
) -> NoReturn:
    raise ProviderError(code, message, remediation)


if __name__ == "__main__":
    raise SystemExit(main())
