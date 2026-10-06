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
