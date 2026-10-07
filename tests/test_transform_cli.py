from __future__ import annotations

import io
import json
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

from datajig.cli import main
from datajig.native import (
    NATIVE_TRANSFORM_COMMANDS,
    TransformProviderUnavailableError,
    run_native,
)
from tests.cli_harness import DataJigCliTestCase


class TransformCliBindingTests(unittest.TestCase):
    def capabilities(self) -> bytes:
        return json.dumps(
            {
                "agent_api_version": 1,
                "backend": "rust",
                "identity_namespace": "datajig-v1",
                "tool": {"name": "datajig"},
                "commands": ["capabilities", "describe", *sorted(NATIVE_TRANSFORM_COMMANDS)],
                "transform_plan_schema_versions": [2],
                "transform_receipt_schema_versions": [1],
                "transform_provider_protocol_versions": [1],
                "transform_source_formats": ["csv", "parquet", "jsonl"],
                "revision_schema_versions": [1, 2, 3],
                "features": {"agent_native_transforms": True},
            }
        ).encode()

    def test_only_transform_commands_receive_the_active_python(self) -> None:
        observed: list[dict[str, str] | None] = []

        def completed(command: list[str], **kwargs: object) -> mock.Mock:
            environment = kwargs.get("env")
            observed.append(environment if isinstance(environment, dict) else None)
            if command[-1] == "capabilities":
                return mock.Mock(returncode=0, stdout=self.capabilities(), stderr=b"")
            return mock.Mock(returncode=0, stdout="{}\n", stderr="")

        with (
            mock.patch("datajig.native._resolve_native_binary", return_value=Path("/native")),
            mock.patch("datajig.native.subprocess.run", side_effect=completed),
            mock.patch.dict(os.environ, {"_DATAJIG_PROVIDER_PYTHON": "/attacker/python"}),
        ):
            run_native(["transform-info", "receipt.json"])
            run_native(["describe"])

        self.assertNotIn("_DATAJIG_PROVIDER_PYTHON", observed[0] or {})
        self.assertEqual(os.path.abspath(sys.executable), observed[1]["_DATAJIG_PROVIDER_PYTHON"])
        self.assertNotIn("_DATAJIG_PROVIDER_PYTHON", observed[2] or {})
        self.assertNotIn("_DATAJIG_PROVIDER_PYTHON", observed[3] or {})

    def test_missing_optional_provider_is_a_machine_readable_install_error(self) -> None:
        message = "DuckDB is unavailable. Install it with: pip install 'datajig[duckdb]'"
        stderr = io.StringIO()
        with (
            mock.patch(
                "datajig.cli.run_native",
                side_effect=TransformProviderUnavailableError(message),
            ),
            mock.patch("sys.stderr", stderr),
        ):
            exit_code = main(["transform-plan"])

        self.assertEqual(2, exit_code)
        error = json.loads(stderr.getvalue())
        self.assertEqual("PROVIDER_UNAVAILABLE", error["error"]["code"])
        self.assertIn("pip install 'datajig[duckdb]'", error["error"]["message"])
        self.assertEqual(
            [{"command": "transform-plan", "args": ["--help"]}],
            error["next_actions"],
        )

    def test_help_does_not_require_the_optional_provider(self) -> None:
        with (
            mock.patch("datajig.native._resolve_native_binary", return_value=Path("/native")),
            mock.patch("datajig.native._verify_native"),
            mock.patch("datajig.native._verify_transform_provider") as verify_provider,
            mock.patch(
                "datajig.native.subprocess.run",
                return_value=mock.Mock(returncode=0, stdout="help\n", stderr=""),
            ),
        ):
            completed = run_native(["transform-plan", "--help"])

        self.assertEqual(0, completed.returncode)
        verify_provider.assert_not_called()


class TransformCliWorkflowTests(DataJigCliTestCase):
    def test_inline_parameters_are_bound_without_being_treated_as_a_path(self) -> None:
        source = self.root / "events.csv"
        source.write_text("id,value\na,1\nb,2\n", encoding="utf-8")
        output = self.root / "prepared.jsonl"
        plan = self.root / "transform-plan.json"

        planned = self.run_cli(
            "transform-plan",
            "--input",
            f"events={source}",
            "--sql",
            "SELECT id, value FROM events WHERE value = ? ORDER BY id",
            "--params",
            '["2"]',
            "--id-field",
            "id",
            "--output",
            output,
            "--plan",
            plan,
        ).payload["artifact"]
        self.run_cli("transform-apply", plan, "--accept-plan", planned["plan_id"])

        self.assertEqual(
            [{"id": "b", "value": "2"}],
            [json.loads(line) for line in output.read_text().splitlines()],
        )

    def test_parameter_file_is_explicit_and_mutually_exclusive_with_inline_json(self) -> None:
        source = self.root / "events.csv"
        source.write_text("id,value\na,1\nb,2\n", encoding="utf-8")
        parameters = self.root / "params.json"
        parameters.write_text('["1"]', encoding="utf-8")
        plan = self.root / "transform-plan.json"

        self.run_cli(
            "transform-plan",
            "--input",
            f"events={source}",
            "--sql",
            "SELECT id, value FROM events WHERE value = ? ORDER BY id",
            "--params-file",
            parameters,
            "--id-field",
            "id",
            "--output",
            self.root / "prepared.jsonl",
            "--plan",
            plan,
        ).payload["artifact"]

        plan_document = json.loads(plan.read_text(encoding="utf-8"))
        self.assertTrue(plan_document["parameter_content_id"].startswith("params_"))
        conflict = self.run_cli(
            "transform-plan",
            "--input",
            f"events={source}",
            "--sql",
            "SELECT id FROM events WHERE value = ? ORDER BY id",
            "--params",
            '["1"]',
            "--params-file",
            parameters,
            "--id-field",
            "id",
            "--output",
            self.root / "conflict.jsonl",
            "--plan",
            self.root / "conflict-plan.json",
            expected_returncode=2,
        ).payload
        self.assertEqual("INVALID_ARGUMENT", conflict["error"]["code"])

    def test_source_row_limit_exposes_machine_readable_details(self) -> None:
        source = self.root / "too-many.csv"
        with source.open("w", encoding="utf-8", newline="\n") as stream:
            stream.write("id,value\n")
            for index in range(2_000_001):
                stream.write(f"{index},x\n")

        failed = self.run_cli(
            "transform-plan",
            "--input",
            f"events={source}",
            "--sql",
            "SELECT id, value FROM events ORDER BY id",
            "--id-field",
            "id",
            "--output",
            self.root / "prepared.jsonl",
            "--plan",
            self.root / "transform-plan.json",
            expected_returncode=2,
        ).payload

        self.assertEqual("SOURCE_LIMIT_EXCEEDED", failed["error"]["code"])
        self.assertEqual(
            {
                "metric": "source_rows",
                "observed": 2_000_001,
                "observed_is_lower_bound": True,
                "limit": 2_000_000,
                "unit": "rows",
            },
            failed["error"]["details"],
        )

    def test_output_field_limit_exposes_machine_readable_details(self) -> None:
        source = self.root / "wide.csv"
        fields = ["id", *(f"value_{index}" for index in range(256))]
        source.write_text(
            ",".join(fields) + "\n" + ",".join(["row-1", *("x" for _ in range(256))]) + "\n",
            encoding="utf-8",
        )

        failed = self.run_cli(
            "transform-plan",
            "--input",
            f"wide={source}",
            "--sql",
            "SELECT * FROM wide ORDER BY id",
            "--id-field",
            "id",
            "--output",
            self.root / "prepared.jsonl",
            "--plan",
            self.root / "transform-plan.json",
            expected_returncode=2,
        ).payload

        self.assertEqual("OUTPUT_LIMIT_EXCEEDED", failed["error"]["code"])
        self.assertEqual(
            {
                "metric": "output_fields",
                "observed": 257,
                "observed_is_lower_bound": False,
                "limit": 256,
                "unit": "fields",
            },
            failed["error"]["details"],
        )

    def test_large_inline_parameter_array_reports_the_contract_not_a_path_error(self) -> None:
        source = self.root / "events.csv"
        source.write_text("id,value\na,1\n", encoding="utf-8")
        failed = self.run_cli(
            "transform-plan",
            "--input",
            f"events={source}",
            "--sql",
            "SELECT id, value FROM events ORDER BY id",
            "--params",
            json.dumps(list(range(257))),
            "--id-field",
            "id",
            "--output",
            self.root / "prepared.jsonl",
            "--plan",
            self.root / "transform-plan.json",
            expected_returncode=2,
        ).payload

        self.assertEqual("INVALID_ARGUMENT", failed["error"]["code"])
        self.assertIn("at most 256 JSON scalars", failed["error"]["message"])
        self.assertNotIn("File name too long", failed["error"]["message"])

    def test_mixed_newline_csv_round_trip_preserves_raw_source_bytes(self) -> None:
        source = self.root / "mixed.csv"
        raw = b'id,text\r\n1,"hello\nworld"\r\n2,plain\n3,"crlf\r\ninside"\r\n'
        source.write_bytes(raw)
        output = self.root / "prepared.jsonl"
        plan = self.root / "transform-plan.json"

        planned = self.run_cli(
            "transform-plan",
            "--input",
            f"mixed={source}",
            "--sql",
            "SELECT id, text FROM mixed ORDER BY id",
            "--id-field",
            "id",
            "--output",
            output,
            "--plan",
            plan,
        ).payload["artifact"]
        self.run_cli("transform-apply", plan, "--accept-plan", planned["plan_id"])

        self.assertEqual(raw, source.read_bytes())
        self.assertEqual(
            [
                {"id": "1", "text": "hello\nworld"},
                {"id": "2", "text": "plain"},
                {"id": "3", "text": "crlf\r\ninside"},
            ],
            [json.loads(line) for line in output.read_text().splitlines()],
        )

    def test_source_load_failure_has_diagnostic_and_next_action(self) -> None:
        source = self.root / "unclosed.csv"
        source.write_bytes(b'id,value\n1,"never closes\n2,x\n')

        failed = self.run_cli(
            "transform-plan",
            "--input",
            f"events={source}",
            "--sql",
            "SELECT id, value FROM events ORDER BY id",
            "--id-field",
            "id",
            "--output",
            self.root / "prepared.jsonl",
            "--plan",
            self.root / "transform-plan.json",
            expected_returncode=2,
        ).payload

        self.assertEqual("SOURCE_LOAD_FAILED", failed["error"]["code"])
        self.assertIn("did not report a line number", failed["error"]["message"])
        self.assertEqual(
            [{"command": "transform-plan", "args": ["--help"]}],
            failed["next_actions"],
        )

    def test_query_conversion_failure_preserves_actionable_diagnostic(self) -> None:
        source = self.root / "duplicate-header.csv"
        source.write_text("id,price\na,10\nid,price\nb,20\n", encoding="utf-8")

        failed = self.run_cli(
            "transform-plan",
            "--input",
            f"events={source}",
            "--sql",
            "SELECT id, CAST(price AS BIGINT) AS price FROM events ORDER BY id",
            "--id-field",
            "id",
            "--output",
            self.root / "prepared.jsonl",
            "--plan",
            self.root / "transform-plan.json",
            expected_returncode=2,
        ).payload

        self.assertEqual("TRANSFORM_QUERY_FAILED", failed["error"]["code"])
        self.assertIn("Conversion Error", failed["error"]["message"])
        self.assertIn("price", failed["error"]["message"])
        self.assertIn("INT64", failed["error"]["message"])
        self.assertNotIn("LINE 1", failed["error"]["message"])
        self.assertLessEqual(len(failed["error"]["message"].encode("utf-8")), 1024)
        self.assertFalse((self.root / "prepared.jsonl").exists())
        self.assertFalse((self.root / "transform-plan.json").exists())
        self.assertEqual(
            [{"command": "transform-plan", "args": ["--help"]}],
            failed["next_actions"],
        )

    def test_sql_file_is_embedded_in_the_plan_and_not_a_live_apply_dependency(self) -> None:
        source = self.root / "events.csv"
        source.write_text("id,value\na,1\nb,2\n", encoding="utf-8")
        sql_file = self.root / "query.sql"
        sql = "SELECT id, CAST(value AS BIGINT) AS value FROM events ORDER BY id"
        sql_file.write_text(sql, encoding="utf-8")
        output = self.root / "prepared.jsonl"
        plan = self.root / "transform-plan.json"

        planned = self.run_cli(
            "transform-plan",
            "--input",
            f"events={source}",
            "--sql-file",
            sql_file,
            "--id-field",
            "id",
            "--output",
            output,
            "--plan",
            plan,
        ).payload["artifact"]
        plan_document = json.loads(plan.read_text(encoding="utf-8"))
        self.assertEqual(2, plan_document["schema_version"])
        self.assertEqual(sql, plan_document["sql"])
        self.assertNotIn("sql_path", plan_document)

        sql_file.unlink()
        applied = self.run_cli(
            "transform-apply", plan, "--accept-plan", planned["plan_id"]
        ).payload["artifact"]

        self.assertFalse(applied["recovered"])
        self.assertFalse(applied["already_applied"])
        self.assertEqual(
            [{"id": "a", "value": 1}, {"id": "b", "value": 2}],
            [json.loads(line) for line in output.read_text().splitlines()],
        )


if __name__ == "__main__":
    unittest.main()
