from __future__ import annotations

import subprocess
import tempfile
import tomllib
import unittest
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CHECKER = REPOSITORY_ROOT / "tools" / "check_repository_hygiene.py"
FORBIDDEN_EMAIL = "dawnkisser" + "@dr.com"


class ReleaseRepositoryHygieneTests(unittest.TestCase):
    def test_release_metadata_is_consistent_and_provider_is_optional(self) -> None:
        metadata = tomllib.loads((REPOSITORY_ROOT / "pyproject.toml").read_text())
        rust_metadata = tomllib.loads(
            (REPOSITORY_ROOT / "rust" / "datajig-core" / "Cargo.toml").read_text()
        )

        self.assertEqual("0.6.1", metadata["project"]["version"])
        self.assertEqual("0.6.1", rust_metadata["package"]["version"])
        self.assertEqual("PYPI.md", metadata["project"]["readme"])
        self.assertEqual(
            [{"name": "Kejun Liu", "email": "liukj7@gmail.com"}],
            metadata["project"]["authors"],
        )
        self.assertNotIn("duckdb", "\n".join(metadata["project"]["dependencies"]).lower())
        self.assertEqual(
            ["duckdb==1.5.6"], metadata["project"]["optional-dependencies"]["duckdb"]
        )

        pypi_readme = (REPOSITORY_ROOT / metadata["project"]["readme"]).read_text(
            encoding="utf-8"
        )
        self.assertNotIn("<img", pypi_readme)
        self.assertNotIn("assets/", pypi_readme)
        self.assertFalse(any("\u4e00" <= character <= "\u9fff" for character in pypi_readme))

    def test_public_tree_contains_only_production_material(self) -> None:
        tracked = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=REPOSITORY_ROOT,
            check=True,
            capture_output=True,
        ).stdout.split(b"\0")
        names = [item.decode("utf-8") for item in tracked if item]

        self.assertIn("src/datajig/providers/duckdb.py", names)
        for name in names:
            self.assertFalse(forbidden_public_path(name), name)
            payload = (REPOSITORY_ROOT / name).read_bytes()
            self.assertNotIn(FORBIDDEN_EMAIL.encode(), payload.lower(), name)

        english_readme = (REPOSITORY_ROOT / "README.md").read_text(encoding="utf-8")
        self.assertFalse(any("\u4e00" <= character <= "\u9fff" for character in english_readme))

    def test_public_readme_uses_a_repository_relative_hero(self) -> None:
        english_readme = (REPOSITORY_ROOT / "README.md").read_text(encoding="utf-8")
        chinese_readme = (REPOSITORY_ROOT / "README.zh-CN.md").read_text(encoding="utf-8")

        for readme in (english_readme, chinese_readme):
            self.assertIn('src="assets/datajig-hero.png"', readme)
            self.assertNotIn("raw.githubusercontent.com", readme)

    def test_workflows_pin_node_24_official_actions(self) -> None:
        workflows = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((REPOSITORY_ROOT / ".github" / "workflows").glob("*.yml"))
        )

        expected = {
            "actions/checkout@fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09",  # v5
            "actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1",  # v6
            "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",  # v7
            "actions/download-artifact@37930b1c2abaa49bbe596cd826c3c89aef350131",  # v7
        }
        legacy = {
            "actions/checkout@11d5960a326750d5838078e36cf38b85af677262",
            "actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065",
            "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
            "actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
        }

        for action in expected:
            self.assertIn(action, workflows)
        for action in legacy:
            self.assertNotIn(action, workflows)


def forbidden_public_path(path: str) -> bool:
    lowered = path.lower()
    parts = Path(path).parts
    return (
        bool(parts)
        and parts[0]
        in {
            ".benchmarks",
            ".datajig",
            ".mypy_cache",
            ".pytest_cache",
            ".ruff_cache",
            ".superpowers",
            ".venv",
            "build",
            "dist",
            "docs",
        }
    ) or any(
        token in lowered
        for token in (
            "__pycache__",
            ".egg-info/",
            "implementation-plan",
            "/progress.md",
            ".pyc",
            ".pyo",
            ".whl",
        )
    )


class RepositoryHygieneTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory(
            prefix="datajig-hygiene-test-"
        )
        self.addCleanup(self._temporary_directory.cleanup)
        self.root = Path(self._temporary_directory.name)
        self.git("init", "-q")
        self.git("config", "user.name", "Kejun Liu")
        self.git("config", "user.email", "liukj7@gmail.com")
        (self.root / "safe.txt").write_text("safe\n", encoding="utf-8")
        self.git("add", "safe.txt")
        self.git("commit", "-q", "-m", "initial safe commit")

    def test_staged_mode_rejects_forced_internal_paths_and_old_email_content(self) -> None:
        internal = self.root / "docs" / "internal.md"
        internal.parent.mkdir()
        internal.write_text("private\n", encoding="utf-8")
        self.git("add", "-f", "docs/internal.md")

        failed = self.check("staged")

        self.assertEqual(1, failed.returncode)
        self.assertIn("docs/internal.md", failed.stderr)

        self.git("reset", "-q", "HEAD", "docs/internal.md")
        (self.root / "safe.txt").write_text(
            f"forbidden identity: {FORBIDDEN_EMAIL}\n", encoding="utf-8"
        )
        self.git("add", "safe.txt")

        failed = self.check("staged")

        self.assertEqual(1, failed.returncode)
        self.assertIn("forbidden email", failed.stderr)

    def test_staged_and_message_modes_require_the_repository_gmail_identity(self) -> None:
        self.git("config", "user.email", FORBIDDEN_EMAIL)

        staged = self.check("staged")
        message = self.root / "COMMIT_EDITMSG"
        message.write_text("safe message\n", encoding="utf-8")
        commit_message = self.check("message", "--message-file", str(message))

        self.assertEqual(1, staged.returncode)
        self.assertEqual(1, commit_message.returncode)
        self.assertIn("liukj7@gmail.com", staged.stderr)

    def test_history_mode_rejects_forbidden_commit_messages(self) -> None:
        (self.root / "safe.txt").write_text("next\n", encoding="utf-8")
        self.git("add", "safe.txt")
        self.git(
            "commit",
            "-q",
            "-m",
            "unsafe trailer",
            "-m",
            f"Co-authored-by: Kejun Liu <{FORBIDDEN_EMAIL}>",
        )

        failed = self.check("history")

        self.assertEqual(1, failed.returncode)
        self.assertIn("commit metadata or message", failed.stderr)

    def test_clean_repository_passes_all_modes(self) -> None:
        message = self.root / "COMMIT_EDITMSG"
        message.write_text("safe message\n", encoding="utf-8")

        self.assertEqual(0, self.check("staged").returncode)
        self.assertEqual(
            0, self.check("message", "--message-file", str(message)).returncode
        )
        self.assertEqual(0, self.check("history").returncode)

    def check(self, mode: str, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python", str(CHECKER), "--mode", mode, *arguments],
            cwd=self.root,
            text=True,
            capture_output=True,
            check=False,
        )

    def git(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *arguments],
            cwd=self.root,
            text=True,
            capture_output=True,
            check=True,
        )


if __name__ == "__main__":
    unittest.main()
