from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path

from tests.cli_harness import REPOSITORY_ROOT, DataJigCliTestCase


class RepositoryCliTests(DataJigCliTestCase):
    def setUp(self) -> None:
        super().setUp()
        completed = subprocess.run(
            ["git", "init", "--quiet", str(self.root)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)

    def git(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment["PATH"] = f"{REPOSITORY_ROOT / '.venv/bin'}:{environment['PATH']}"
        environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "src")
        environment["DATAJIG_NATIVE"] = str(self.native)
        return subprocess.run(
            ["git", *arguments],
            cwd=self.root,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )

    def test_python_entrypoint_installs_checks_and_detects_tampering(self) -> None:
        installed = self.run_cli(
            "repository-install",
            "--root",
            self.root,
            "--state",
            ".datajig-training",
        )
        self.assertEqual("repository_integration_installed", installed.payload["kind"])
        self.assertEqual("installed", installed.payload["decision"])
        self.assertTrue((self.root / ".datajig-repository.json").is_file())
        self.assertTrue((self.root / ".agents/skills/datajig/SKILL.md").is_file())
        self.assertTrue((self.root / ".github/workflows/datajig.yml").is_file())
        self.assertTrue((self.root / ".githooks/pre-commit").is_file())

        # A bound workspace is mandatory before the repository can be ready.
        self.assert_cli_error("REPOSITORY_IO_ERROR", "repository-check", "--root", self.root)

        # Reinstall without a state cannot silently remove the binding.
        self.assert_cli_error("REPOSITORY_CONFLICT", "repository-install", "--root", self.root)

    def test_python_entrypoint_checks_unbound_repository_and_reports_drift(self) -> None:
        self.run_cli("repository-install", "--root", self.root)
        repeated = self.run_cli("repository-install", "--root", self.root)
        self.assertEqual("unchanged", repeated.payload["decision"])
        checked = self.run_cli("repository-check", "--root", self.root)
        self.assertEqual("repository_integration_checked", checked.payload["kind"])
        self.assertEqual("ready", checked.payload["decision"])

        skill = self.root / ".agents/skills/datajig/SKILL.md"
        skill.write_text("tampered\n", encoding="utf-8")
        self.assert_cli_error("REPOSITORY_CONFLICT", "repository-check", "--root", self.root)

    def test_real_pre_commit_hook_rejects_dirty_bound_workspace(self) -> None:
        dataset, _ = self.initialize_jsonl([{"id": "one", "text": "ready"}])
        self.run_cli("repository-install", "--root", self.root, "--state", ".datajig")
        self.assertEqual(0, self.git("config", "user.name", "DataJig Test").returncode)
        self.assertEqual(0, self.git("config", "user.email", "liukj7@gmail.com").returncode)
        self.assertEqual(0, self.git("add", ".").returncode)
        accepted = self.git("commit", "-m", "clean baseline")
        self.assertEqual(0, accepted.returncode, accepted.stderr)

        original = dataset.read_text(encoding="utf-8")
        dataset.write_text('{"id":"one","text":"staged-only-dirty"}\n', encoding="utf-8")
        self.assertEqual(0, self.git("add", str(dataset)).returncode)
        dataset.write_text(original, encoding="utf-8")
        staged_only = self.git("commit", "-m", "staged-only dirty dataset")
        self.assertNotEqual(0, staged_only.returncode)
        self.assertIn("INVALID_REPOSITORY", staged_only.stderr)
        self.assertEqual(0, self.git("restore", "--staged", str(dataset)).returncode)

        self.assertEqual(0, self.git("rm", "--cached", str(dataset)).returncode)
        staged_delete = self.git("commit", "-m", "staged dataset deletion")
        self.assertNotEqual(0, staged_delete.returncode)
        self.assertIn("INVALID_REPOSITORY", staged_delete.stderr)
        self.assertEqual(0, self.git("restore", "--staged", str(dataset)).returncode)

        dataset.write_text('{"id":"one","text":"dirty"}\n', encoding="utf-8")
        self.assertEqual(0, self.git("add", str(dataset)).returncode)
        rejected = self.git("commit", "-m", "dirty dataset")
        self.assertNotEqual(0, rejected.returncode)
        self.assertIn("INVALID_REPOSITORY", rejected.stderr)

    def test_fresh_clone_ci_skips_only_uncommitted_hook_activation(self) -> None:
        self.run_cli("repository-install", "--root", self.root)
        self.assertEqual(0, self.git("config", "user.name", "DataJig Test").returncode)
        self.assertEqual(0, self.git("config", "user.email", "liukj7@gmail.com").returncode)
        self.assertEqual(0, self.git("add", ".").returncode)
        self.assertEqual(0, self.git("commit", "-m", "repository integration").returncode)

        clone_directory = tempfile.TemporaryDirectory(prefix="datajig-clone-test-")
        self.addCleanup(clone_directory.cleanup)
        clone = Path(clone_directory.name) / "fresh clone 研究"
        completed = subprocess.run(
            ["git", "clone", "--quiet", str(self.root), str(clone)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assert_cli_error("REPOSITORY_CONFLICT", "repository-check", "--root", clone)
        checked = self.run_cli("repository-check", "--root", clone, "--ci")
        self.assertEqual("ready", checked.payload["decision"])

    def test_hook_rejects_partial_staged_deletion_in_directory_dataset(self) -> None:
        dataset = self.root / "images"
        dataset.mkdir()
        first = dataset / "first.txt"
        second = dataset / "second.txt"
        first.write_text("first\n", encoding="utf-8")
        second.write_text("second\n", encoding="utf-8")
        self.run_cli("init", dataset, "--state", ".datajig-images")
        self.run_cli(
            "repository-install",
            "--root",
            self.root,
            "--state",
            ".datajig-images",
        )
        self.assertEqual(0, self.git("config", "user.name", "DataJig Test").returncode)
        self.assertEqual(0, self.git("config", "user.email", "liukj7@gmail.com").returncode)
        self.assertEqual(0, self.git("add", ".").returncode)
        self.assertEqual(0, self.git("commit", "-m", "directory baseline").returncode)

        self.assertEqual(0, self.git("rm", "--cached", str(first)).returncode)
        rejected = self.git("commit", "-m", "partial staged deletion")
        self.assertNotEqual(0, rejected.returncode)
        self.assertIn("INVALID_REPOSITORY", rejected.stderr)


if __name__ == "__main__":
    import unittest

    unittest.main()
