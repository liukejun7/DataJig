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


class TransformCliBindingTests(unittest.TestCase):
    def capabilities(self) -> bytes:
        return json.dumps(
            {
                "agent_api_version": 1,
                "backend": "rust",
                "identity_namespace": "datajig-v1",
                "tool": {"name": "datajig"},
                "commands": ["capabilities", "describe", *sorted(NATIVE_TRANSFORM_COMMANDS)],
                "transform_plan_schema_versions": [1],
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


if __name__ == "__main__":
    unittest.main()
