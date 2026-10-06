from __future__ import annotations

import os
import subprocess
import unittest
from importlib.metadata import version
from pathlib import Path

from cli_harness import REPOSITORY_ROOT


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
        self.assertIn("basis-point", export.stdout)
        self.assertIn("--split train=7000 --split val=2000 --split test=1000", export.stdout)

    def test_top_level_help_lists_transform_workflow(self) -> None:
        completed = self.run_help("")
        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertIn("transform-plan", completed.stdout)
        self.assertIn("transform-apply", completed.stdout)
        self.assertIn("transform-info", completed.stdout)

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
