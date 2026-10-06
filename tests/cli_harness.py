from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class CliResult:
    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    payload: dict[str, Any]


class DataJigCliTestCase(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory(prefix="datajig-cli-test-")
        self.addCleanup(self._temporary_directory.cleanup)
        self.root = Path(self._temporary_directory.name)
        self.state = self.root / ".datajig"
        configured_native = os.environ.get("DATAJIG_NATIVE")
        native = (
            Path(configured_native)
            if configured_native is not None
            else REPOSITORY_ROOT / "rust" / "target" / "debug" / "datajig-core"
        )
        self.native = native.expanduser().resolve()
        self.assertTrue(self.native.is_file(), f"native backend does not exist: {self.native}")

    def run_cli(
        self,
        *arguments: object,
        expected_returncode: int = 0,
        extra_env: dict[str, str] | None = None,
    ) -> CliResult:
        command = ("python", "-m", "datajig.cli", *(str(item) for item in arguments))
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "src")
        environment["DATAJIG_NATIVE"] = str(self.native)
        if extra_env:
            environment.update(extra_env)
        completed = subprocess.run(
            command,
            cwd=self.root,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )
        self.assertEqual(
            expected_returncode,
            completed.returncode,
            f"command: {' '.join(command)}\nstdout: {completed.stdout}\nstderr: {completed.stderr}",
        )
        serialized = completed.stdout if completed.returncode == 0 else completed.stderr
        try:
            payload = json.loads(serialized)
        except json.JSONDecodeError as error:
            self.fail(
                f"command did not emit one JSON document: {' '.join(command)}\n"
                f"stdout: {completed.stdout}\nstderr: {completed.stderr}\nerror: {error}"
            )
        self.assertIsInstance(payload, dict)
        return CliResult(
            command=command,
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            payload=payload,
        )

    def assert_cli_error(self, code: str, *arguments: object) -> CliResult:
        result = self.run_cli(*arguments, expected_returncode=2)
        self.assertEqual(code, result.payload["error"]["code"])
        return result

    def write_jsonl(self, path: Path, records: list[dict[str, Any]]) -> bytes:
        payload = b"".join(
            json.dumps(record, sort_keys=True, separators=(",", ":")).encode() + b"\n"
            for record in records
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return payload

    def initialize_jsonl(
        self,
        records: list[dict[str, Any]],
        *,
        policy: Path | None = None,
    ) -> tuple[Path, dict[str, Any]]:
        dataset = self.root / "dataset.jsonl"
        self.write_jsonl(dataset, records)
        arguments: list[object] = ["init", dataset, "--id-field", "id", "--state", self.state]
        if policy is not None:
            arguments.extend(("--policy", policy))
        result = self.run_cli(*arguments)
        return dataset, result.payload["artifact"]

    def begin_change(self, task_id: str = "regression-task") -> tuple[str, dict[str, Any]]:
        result = self.run_cli(
            "changeset-begin",
            "--state",
            self.state,
            "--intent",
            "Exercise the public CLI contract",
            "--task-id",
            task_id,
        )
        artifact = result.payload["artifact"]
        return artifact["change_id"], artifact

    def stage_change(self, change_id: str) -> tuple[str, dict[str, Any]]:
        result = self.run_cli(
            "changeset-stage", "--state", self.state, "--change", change_id
        )
        artifact = result.payload["artifact"]
        return artifact["changeset_id"], artifact

    def check_change(self, change_id: str, changeset_id: str) -> dict[str, Any]:
        result = self.run_cli(
            "check",
            "--state",
            self.state,
            "--change",
            change_id,
            "--changeset",
            changeset_id,
        )
        return result.payload["artifact"]

    def status(self) -> dict[str, Any]:
        return self.run_cli("status", "--state", self.state).payload["artifact"]

    def assert_no_staging_artifacts(self) -> None:
        leaked = [path for path in self.root.rglob(".datajig-*")]
        self.assertEqual([], leaked)
