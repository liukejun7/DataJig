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

        self.assertEqual("0.6.0", metadata["project"]["version"])
        self.assertEqual("0.6.0", rust_metadata["package"]["version"])
        self.assertEqual(
            [{"name": "Kejun Liu", "email": "liukj7@gmail.com"}],
            metadata["project"]["authors"],
        )
        self.assertNotIn("duckdb", "\n".join(metadata["project"]["dependencies"]).lower())
        self.assertEqual(
            ["duckdb==1.5.6"], metadata["project"]["optional-dependencies"]["duckdb"]
        )

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
