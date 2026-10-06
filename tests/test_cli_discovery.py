from __future__ import annotations

import json
import os
import subprocess
import unittest
from importlib.metadata import version
from pathlib import Path

from tests.cli_harness import REPOSITORY_ROOT


class CliDiscoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        configured_native = os.environ.get("DATAJIG_NATIVE")
        native = (
            Path(configured_native)
            if configured_native is not None
            else REPOSITORY_ROOT / "rust" / "target" / "debug" / "datajig-core"
        )
        self.native = native.expanduser().resolve()
        self.assertTrue(self.native.is_file(), f"native backend does not exist: {self.native}")

    def run_help(self, command: str) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "src")
        environment["DATAJIG_NATIVE"] = str(self.native)
        arguments = ("python", "-m", "datajig.cli", "--help")
        if command:
            arguments = ("python", "-m", "datajig.cli", command, "--help")
        return subprocess.run(
            arguments,
            cwd=REPOSITORY_ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )

    def test_native_help_survives_the_python_entrypoint(self) -> None:
        check = self.run_help("check")
        self.assertEqual(0, check.returncode, check.stderr)
        self.assertIn("@active", check.stdout)
        self.assertIn("@latest", check.stdout)

        export = self.run_help("export")
        self.assertEqual(0, export.returncode, export.stderr)
        self.assertIn("NAME=WEIGHT", export.stdout)
        self.assertIn("relative integer weight", export.stdout)
        self.assertIn("--split train=7 --split val=2 --split test=1", export.stdout)

        transform = self.run_help("transform-plan")
        self.assertEqual(0, transform.returncode, transform.stderr)
        self.assertIn("--sql <SQL>", transform.stdout)
        self.assertIn("--sql-file <SQL_FILE>", transform.stdout)
        self.assertIn("CSV columns are strings", transform.stdout)

        prepare = self.run_help("prepare-plan")
        self.assertEqual(0, prepare.returncode, prepare.stderr)
        self.assertIn("datajig artifact-schema prepare-recipe", prepare.stdout)
        self.assertIn('"namespace":"datajig"', prepare.stdout)

    def test_top_level_help_lists_transform_workflow(self) -> None:
        completed = self.run_help("")
        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertIn("transform-plan", completed.stdout)
        self.assertIn("transform-apply", completed.stdout)
        self.assertIn("transform-info", completed.stdout)
        self.assertIn("review-plan", completed.stdout)
        self.assertIn("pipeline", completed.stdout)
        self.assertIn("lineage", completed.stdout)
        self.assertNotRegex(completed.stdout, r"(?m)^  plan\s")
        self.assertIn(
            "prepare/import -> transform -> version -> review/seal -> export -> consume",
            completed.stdout,
        )

    def test_capabilities_publish_python_pipeline_contract(self) -> None:
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "src")
        environment["DATAJIG_NATIVE"] = str(self.native)
        completed = subprocess.run(
            ("python", "-m", "datajig.cli", "capabilities"),
            cwd=REPOSITORY_ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertEqual([1], payload["pipeline_plan_schema_versions"])
        self.assertEqual([1], payload["pipeline_receipt_schema_versions"])
        self.assertEqual([1], payload["pipeline_lineage_schema_versions"])
        self.assertTrue(payload["features"]["recoverable_pipeline"])
        self.assertEqual("flat", payload["artifact_file_shape"])
        self.assertEqual("agent_envelope_v1", payload["cli_response_shape"])
        self.assertEqual(
            ["linux", "macos"], payload["delivery_update"]["atomic_exchange_platforms"]
        )
        self.assertTrue(payload["delivery_update"]["filesystem_preflight"])
        self.assertEqual("none", payload["delivery_update"]["fallback"])
        self.assertTrue(payload["features"]["fixed_delivery_updates"])

    def test_top_level_version_matches_installed_package_metadata(self) -> None:
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "src")
        completed = subprocess.run(
            ("python", "-m", "datajig.cli", "--version"),
            cwd=REPOSITORY_ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )

        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertEqual(f"datajig {version('datajig')}\n", completed.stdout)
        self.assertEqual("", completed.stderr)


if __name__ == "__main__":
    unittest.main()
