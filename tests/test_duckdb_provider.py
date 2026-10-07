from __future__ import annotations

import json
import math
import subprocess
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path
from typing import ClassVar
from unittest import mock

import blake3

try:
    import duckdb
except ModuleNotFoundError:
    duckdb = None  # type: ignore[assignment]

from datajig.providers import duckdb as provider


def _limits() -> dict[str, int]:
    return {
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


class DuckDbPackagingTest(unittest.TestCase):
    def test_optional_extra_is_exactly_pinned(self) -> None:
        metadata = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
        self.assertEqual(["duckdb==1.5.6"], metadata["project"]["optional-dependencies"]["duckdb"])
        self.assertNotIn("duckdb", " ".join(metadata["project"]["dependencies"]).lower())

    def test_source_checkout_does_not_report_a_stale_release_version(self) -> None:
        with mock.patch.object(
            provider.metadata,
            "version",
            side_effect=provider.metadata.PackageNotFoundError,
        ):
            self.assertEqual("0+unknown", provider._implementation_version())


@unittest.skipIf(duckdb is None, "optional DuckDB provider is not installed")
class DuckDbProviderTest(unittest.TestCase):
    def test_probe_matches_rust_provider_identity(self) -> None:
        identity = provider._probe()
        self.assertEqual("datajig.transform-provider.v1", identity["protocol"])
        self.assertEqual(1, identity["protocol_version"])
        self.assertEqual("datajig-duckdb-python", identity["implementation"])
        self.assertEqual("1.5.6", identity["duckdb_version"])
        self.assertEqual(1, identity["serializer_version"])
        self.assertEqual(2, identity["source_loader_policy_version"])
        self.assertEqual(1, identity["sql_policy_version"])
        payload = {key: value for key, value in identity.items() if key != "provider_id"}
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        expected = blake3.blake3(b"datajig-transform-provider-v1\0" + encoded).hexdigest()
        self.assertEqual(f"provider_{expected}", identity["provider_id"])

    def test_isolated_module_probe_emits_one_bounded_response(self) -> None:
        request = {
            "protocol": "datajig.transform-provider.v1",
            "protocol_version": 1,
            "correlation_id": "probe",
            "operation": "probe",
            "limits": _limits(),
        }
        completed = subprocess.run(
            [sys.executable, "-I", "-m", "datajig.providers.duckdb"],
            input=json.dumps(request, separators=(",", ":")),
            text=True,
            capture_output=True,
            env={},
            check=False,
            timeout=5,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertEqual("", completed.stderr)
        self.assertLess(len(completed.stdout.encode("utf-8")), 1024 * 1024)
        self.assertEqual(1, len(completed.stdout.splitlines()))
        response = json.loads(completed.stdout)
        self.assertEqual("ok", response["status"])
        self.assertTrue(response["lockdown_supported"])
        self.assertEqual(provider._probe(), response["provider"])

    def test_loaders_use_bound_staged_paths_and_memory_tables(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            csv_path = root / "events.csv"
            csv_path.write_text("id,value\n001,7\n", encoding="utf-8")
            parquet_path = root / "labels.parquet"
            writer = duckdb.connect(":memory:")
            writer.execute(
                "COPY (SELECT 2::BIGINT AS id, true AS accepted, 1.5::DOUBLE AS score) "
                "TO ? (FORMAT PARQUET)",
                [str(parquet_path)],
            )
            writer.close()
            jsonl_path = root / "notes.jsonl"
            jsonl_path.write_text('{"id":3,"note":"ok"}\n{"id":4}\n', encoding="utf-8")
            request = {
                "sources": [
                    _source("events", csv_path, "csv", 1),
                    _source("labels", parquet_path, "parquet", 1),
                    _source("notes", jsonl_path, "jsonl", 2),
                ]
            }
            connection = duckdb.connect(":memory:")
            provider._load_sources(connection, request)
            self.assertEqual([("001", "7")], connection.execute("SELECT * FROM events").fetchall())
            self.assertEqual(
                [(2, True, 1.5)], connection.execute("SELECT * FROM labels").fetchall()
            )
            self.assertEqual(
                [(3, "ok"), (4, None)],
                connection.execute("SELECT id, note FROM notes ORDER BY id").fetchall(),
            )
            tables = connection.execute(
                "SELECT table_name FROM information_schema.tables ORDER BY table_name"
            ).fetchall()
            self.assertEqual([("events",), ("labels",), ("notes",)], tables)
            databases = connection.execute("PRAGMA database_list").fetchall()
            self.assertEqual(1, len(databases))
            self.assertEqual(("memory", None), databases[0][1:])
            connection.close()

            nested = root / "nested.jsonl"
            nested.write_text('{"id":1,"value":{"escape":true}}\n', encoding="utf-8")
            connection = duckdb.connect(":memory:")
            with self.assertRaisesRegex(provider.ProviderError, "flat scalar"):
                provider._load_sources(
                    connection, {"sources": [_source("nested", nested, "jsonl", 1)]}
                )
            connection.close()

    def test_csv_loader_accepts_mixed_record_endings_and_quoted_newlines(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "mixed.csv"
            source.write_bytes(
                b'id,text\r\n1,"hello\nworld"\r\n2,plain\n3,"crlf\r\ninside"\r\n'
            )
            connection = duckdb.connect(":memory:")

            provider._load_sources(
                connection, {"sources": [_source("mixed", source, "csv", 3)]}
            )

            self.assertEqual(
                [("1", "hello\nworld"), ("2", "plain"), ("3", "crlf\r\ninside")],
                connection.execute("SELECT id, text FROM mixed ORDER BY id").fetchall(),
            )
            connection.close()

    def test_csv_loader_reports_real_line_numbers_without_leaking_staged_paths(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "private-name.csv"
            source.write_bytes(b"id,value\n1,\xff\n")
            connection = duckdb.connect(":memory:")

            with self.assertRaises(provider.ProviderError) as raised:
                provider._load_sources(
                    connection, {"sources": [_source("events", source, "csv", 1)]}
                )

            self.assertEqual("SOURCE_LOAD_FAILED", raised.exception.code)
            self.assertIn("source 'events' at line 2", raised.exception.message)
            self.assertNotIn(str(source), raised.exception.message)
            connection.close()

    def test_csv_loader_does_not_invent_a_line_number(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "unclosed.csv"
            source.write_bytes(b'id,value\n1,"never closes\n2,x\n')
            connection = duckdb.connect(":memory:")

            with self.assertRaises(provider.ProviderError) as raised:
                provider._load_sources(
                    connection, {"sources": [_source("events", source, "csv", 2)]}
                )

            self.assertIn("source 'events'", raised.exception.message)
            self.assertIn("did not report a line number", raised.exception.message)
            self.assertNotRegex(raised.exception.message, r"at line \d+")
            connection.close()

    def test_lockdown_is_verified_before_user_query(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            source.write_text("id\n1\n", encoding="utf-8")
            temp = root / "temp"
            temp.mkdir()
            connection = duckdb.connect(":memory:")
            provider._lockdown(connection, temp, 512 * 1024 * 1024)
            expected = {
                "enable_external_access": False,
                "autoinstall_known_extensions": False,
                "autoload_known_extensions": False,
                "allow_persistent_secrets": False,
                "python_enable_replacements": False,
                "threads": 1,
                "temp_directory": str(temp.resolve()),
            }
            for setting, value in expected.items():
                observed = connection.execute(
                    f"SELECT current_setting('{setting}')"
                ).fetchone()[0]
                self.assertEqual(value, observed)
            self.assertEqual("512.0 MiB", connection.execute(
                "SELECT current_setting('memory_limit')"
            ).fetchone()[0])
            self.assertEqual([], connection.execute("SELECT * FROM duckdb_secrets()").fetchall())
            self.assertTrue(connection.execute(
                "SELECT current_setting('lock_configuration')"
            ).fetchone()[0])
            with self.assertRaises(duckdb.PermissionException):
                connection.execute("SELECT * FROM read_csv(?)", [str(source)]).fetchall()
            with self.assertRaises(duckdb.Error):
                connection.execute("INSTALL httpfs")
            with self.assertRaises(duckdb.Error):
                connection.execute("SET enable_external_access = true")
            connection.close()

    def test_stream_result_uses_bounded_batches_without_fetchall(self) -> None:
        cursor = _StreamingCursor(130_000)
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.jsonl"
            result = provider._stream_result(cursor, candidate, 65_536)
            self.assertEqual(130_000, result["rows"])
            self.assertEqual(candidate.stat().st_size, result["bytes"])
            self.assertEqual([65_536, 65_536, 65_536], cursor.fetch_sizes)
            self.assertFalse(cursor.fetchall_called)
            with candidate.open("rb") as stream:
                self.assertEqual(b'{"id":0,"score":0}\n', stream.readline())
                stream.seek(-30, 2)
                self.assertIn(b'"id":129999', stream.read())

    def test_stream_result_reports_machine_readable_row_limit_details(self) -> None:
        cursor = _StreamingCursor(2)
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.jsonl"
            with self.assertRaises(provider.ProviderError) as raised:
                provider._stream_result(
                    cursor,
                    candidate,
                    65_536,
                    max_rows=1,
                )

        self.assertEqual("OUTPUT_LIMIT_EXCEEDED", raised.exception.code)
        self.assertEqual(
            {
                "metric": "output_rows",
                "observed": 2,
                "observed_is_lower_bound": True,
                "limit": 1,
                "unit": "rows",
            },
            raised.exception.details,
        )

    def test_stream_result_reports_machine_readable_field_limit_details(self) -> None:
        cursor = _WideCursor(257)
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.jsonl"
            with self.assertRaises(provider.ProviderError) as raised:
                provider._stream_result(cursor, candidate, 65_536, max_fields=256)

        self.assertEqual("OUTPUT_LIMIT_EXCEEDED", raised.exception.code)
        self.assertEqual(
            {
                "metric": "output_fields",
                "observed": 257,
                "observed_is_lower_bound": False,
                "limit": 256,
                "unit": "fields",
            },
            raised.exception.details,
        )

    def test_stream_result_reports_machine_readable_byte_limit_details(self) -> None:
        cursor = _StreamingCursor(1)
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.jsonl"
            with self.assertRaises(provider.ProviderError) as raised:
                provider._stream_result(cursor, candidate, 65_536, max_bytes=1)

        self.assertEqual("OUTPUT_LIMIT_EXCEEDED", raised.exception.code)
        details = raised.exception.details
        self.assertIsNotNone(details)
        assert details is not None
        self.assertEqual("output_bytes", details["metric"])
        self.assertGreater(details["observed"], 1)
        self.assertTrue(details["observed_is_lower_bound"])
        self.assertEqual(1, details["limit"])
        self.assertEqual("bytes", details["unit"])

    def test_execute_binds_parameters_and_returns_only_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "events.csv"
            source.write_text("id,value\n1,alpha\n2,beta\n", encoding="utf-8")
            temp = root / "temp"
            temp.mkdir()
            candidate = root / "candidate.jsonl"
            identity = provider._probe()
            request = {
                "protocol": "datajig.transform-provider.v1",
                "protocol_version": 1,
                "correlation_id": "corr-test",
                "operation": "execute",
                "expected_provider": identity,
                "sources": [_source("events", source, "csv", 2)],
                "sql": (
                    "SELECT id, value, current_setting('memory_limit') AS memory_limit, "
                    "current_setting('max_temp_directory_size') AS temp_limit "
                    "FROM events WHERE id = ? ORDER BY id"
                ),
                "parameters": ["2"],
                "id_field": "id",
                "candidate_path": str(candidate),
                "temp_directory": str(temp),
                "limits": {**_limits(), "duckdb_memory_bytes": 64 * 1024 * 1024},
                "ast_policy_digest": "policy_test",
            }
            response = provider._handle_request(request)
            self.assertEqual("ok", response["status"])
            self.assertEqual("corr-test", response["correlation_id"])
            self.assertEqual(identity, response["provider"])
            self.assertNotIn("records", response)
            self.assertEqual(
                '{"id":"2","value":"beta","memory_limit":"64.0 MiB",'
                '"temp_limit":"64.0 MiB"}\n',
                candidate.read_text(encoding="utf-8"),
            )

    def test_query_conversion_failure_preserves_bounded_duckdb_diagnostic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "events.csv"
            source.write_text("id,price\na,10\nid,price\nb,20\n", encoding="utf-8")
            temp = root / "temp"
            temp.mkdir()
            candidate = root / "candidate.jsonl"
            identity = provider._probe()

            response = provider._handle_request(
                {
                    "protocol": "datajig.transform-provider.v1",
                    "protocol_version": 1,
                    "correlation_id": "corr-conversion",
                    "operation": "execute",
                    "expected_provider": identity,
                    "sources": [_source("events", source, "csv", 3)],
                    "sql": (
                        "SELECT id, CAST(price AS BIGINT) AS price "
                        "FROM events ORDER BY id"
                    ),
                    "parameters": [],
                    "id_field": "id",
                    "candidate_path": str(candidate),
                    "temp_directory": str(temp),
                    "limits": _limits(),
                    "ast_policy_digest": "policy_test",
                }
            )

            self.assertEqual("error", response["status"])
            self.assertEqual("QUERY_EXECUTION_FAILED", response["error"]["code"])
            self.assertIn("Conversion Error", response["error"]["message"])
            self.assertIn("price", response["error"]["message"])
            self.assertIn("INT64", response["error"]["message"])
            self.assertNotIn("LINE 1", response["error"]["message"])
            self.assertLessEqual(len(response["error"]["message"].encode("utf-8")), 1024)
            self.assertFalse(candidate.exists())

    def test_missing_duckdb_is_a_structured_provider_error(self) -> None:
        with mock.patch.object(
            provider, "_import_duckdb", side_effect=ModuleNotFoundError("No module named duckdb")
        ):
            response = provider._handle_request(
                {
                    "protocol": "datajig.transform-provider.v1",
                    "protocol_version": 1,
                    "correlation_id": "probe",
                    "operation": "probe",
                    "limits": _limits(),
                }
            )
        self.assertEqual("error", response["status"])
        self.assertEqual("PROVIDER_UNAVAILABLE", response["error"]["code"])
        self.assertIn("datajig[duckdb]", response["error"]["remediation"])

    def test_protocol_scalars_reject_nonfinite_and_out_of_range_numbers(self) -> None:
        for value in [math.nan, math.inf, -math.inf, -(2**63) - 1, 2**64]:
            with self.subTest(value=value):
                self.assertFalse(provider._is_scalar(value))
        for value in [None, False, -(2**63), 2**64 - 1, 1.25, "value"]:
            with self.subTest(value=value):
                self.assertTrue(provider._is_scalar(value))


def _source(alias: str, path: Path, source_format: str, rows: int) -> dict[str, object]:
    return {
        "alias": alias,
        "path": str(path.resolve()),
        "format": source_format,
        "content_id": f"source_{alias}",
        "bytes": path.stat().st_size,
        "rows": rows,
    }


class _StreamingCursor:
    description: ClassVar[list[tuple[object, ...]]] = [
        ("id", "BIGINT", None, None, None, None, False),
        ("score", "DOUBLE", None, None, None, None, True),
    ]

    def __init__(self, rows: int) -> None:
        self.rows = rows
        self.offset = 0
        self.fetch_sizes: list[int] = []
        self.fetchall_called = False

    def fetchmany(self, size: int) -> list[tuple[int, float]]:
        self.fetch_sizes.append(size)
        end = min(self.offset + size, self.rows)
        rows = [
            (index, -0.0 if index == 0 else math.fmod(index, 17))
            for index in range(self.offset, end)
        ]
        self.offset = end
        return rows

    def fetchall(self) -> list[object]:
        self.fetchall_called = True
        raise AssertionError("provider must never call fetchall")


class _WideCursor:
    def __init__(self, fields: int) -> None:
        self.description = [
            (f"field_{index}", "VARCHAR", None, None, None, None, True)
            for index in range(fields)
        ]

    def fetchmany(self, _size: int) -> list[object]:
        raise AssertionError("field limits must be enforced before rows are fetched")
