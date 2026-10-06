from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

from tests.cli_harness import REPOSITORY_ROOT, DataJigCliTestCase


class PatchTransactionTests(DataJigCliTestCase):
    def _prepare_patch(self) -> tuple[Path, bytes, str, str, Path, Path, str]:
        policy = self.root / "policy.json"
        policy.write_text(
            json.dumps(
                {
                    "namespace": "datajig",
                    "schema_version": 1,
                    "adapter": "jsonl",
                    "mode": "full",
                    "fields": {
                        "score": {
                            "required": True,
                            "types": ["number"],
                            "minimum": 0,
                            "maximum": 1,
                        }
                    },
                }
            )
        )
        dataset, _ = self.initialize_jsonl([{"id": "alpha", "score": 0.5}], policy=policy)
        change_id, _ = self.begin_change("patch-regression")
        invalid_candidate = self.write_jsonl(dataset, [{"id": "alpha", "score": 2}])
        changeset_id, _ = self.stage_change(change_id)
        review = self.check_change(change_id, changeset_id)
        self.assertEqual("fix", review["decision"])
        report = Path(review["report_path"])
        findings = self.run_cli(
            "findings", report, "--code", "JSONL_POLICY_RANGE"
        ).payload["findings"]
        self.assertEqual(1, len(findings))
        finding_id = findings[0]["id"]
        request = self.root / "repair.json"
        self.run_cli(
            "patch-draft",
            report,
            finding_id,
            "--state",
            self.state,
            "--change",
            change_id,
            "--changeset",
            changeset_id,
            "--after-json",
            "0.75",
            "--output",
            request,
        )
        preview = self.run_cli(
            "patch-preview",
            request,
            report,
            "--state",
            self.state,
            "--change",
            change_id,
            "--changeset",
            changeset_id,
        ).payload["artifact"]
        return (
            dataset,
            invalid_candidate,
            change_id,
            changeset_id,
            report,
            request,
            preview["patch_id"],
        )

    def _apply_arguments(
        self,
        request: Path,
        report: Path,
        change_id: str,
        changeset_id: str,
        patch_id: str,
    ) -> tuple[Any, ...]:
        return (
            "patch-apply",
            request,
            report,
            "--state",
            self.state,
            "--change",
            change_id,
            "--changeset",
            changeset_id,
            "--accept-patch",
            patch_id,
        )

    def _run_failpoint(self, failpoint: str, *arguments: object) -> None:
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "src")
        environment["DATAJIG_NATIVE"] = str(self.native)
        environment["DATAJIG_TEST_PATCH_FAILPOINT"] = failpoint
        command = ("python", "-m", "datajig.cli", *(str(item) for item in arguments))
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
            86,
            completed.returncode,
            f"command: {' '.join(command)}\n"
            f"stdout: {completed.stdout}\nstderr: {completed.stderr}",
        )
        self.assertEqual("", completed.stdout)
        self.assertEqual("", completed.stderr)

    def _transaction_journal(self) -> tuple[Path, dict[str, Any]]:
        transaction_root = self.state / "private" / "patch-transactions"
        transaction_directories = sorted(
            path for path in transaction_root.iterdir() if path.is_dir()
        )
        self.assertEqual(1, len(transaction_directories))
        transaction = transaction_directories[0]
        self.assertEqual(
            {"journal.json", "preimage.bin"},
            {path.name for path in transaction.iterdir()},
        )
        journal = json.loads((transaction / "journal.json").read_text())
        self.assertEqual(transaction.name, journal["undo_id"])
        return transaction, journal

    def _receipt_payloads(self, directory: str) -> list[dict[str, Any]]:
        receipt_root = self.state / "objects" / directory
        if not receipt_root.exists():
            return []
        return [
            json.loads(path.read_text())
            for path in sorted(receipt_root.glob("*.json"))
        ]

    def _assert_staged_status(
        self,
        change_id: str,
        changeset_id: str,
        expected_head: str,
        expected_state: str,
    ) -> None:
        status = self.run_cli(
            "status",
            "--state",
            self.state,
            "--change",
            change_id,
            "--changeset",
            changeset_id,
        ).payload["artifact"]
        self.assertEqual(expected_head, status["head_revision_id"])
        self.assertEqual(expected_state, status["current_state_id"])
        self.assertEqual(expected_state, status["staged_state_id"])
        self.assertFalse(status["clean"])
        self.assertEqual(1, status["changed_files"])
        self.assertFalse(status["unstaged_changes"])

    def _assert_no_patch_staging_leaks(self) -> None:
        leaked = sorted(
            str(path.relative_to(self.root))
            for path in self.root.rglob("*")
            if path.is_file() and ".datajig-" in path.name
        )
        self.assertEqual([], leaked)
        self.assert_no_staging_artifacts()

    def _assert_apply_recovery(self, failpoint: str, expected_outcome: str) -> None:
        (
            dataset,
            invalid_candidate,
            change_id,
            changeset_id,
            report,
            request,
            patch_id,
        ) = self._prepare_patch()
        expected_applied = (
            json.dumps(
                {"id": "alpha", "score": 0.75}, sort_keys=True, separators=(",", ":")
            ).encode()
            + b"\n"
        )
        original_head = self.status()["head_revision_id"]
        arguments = self._apply_arguments(
            request, report, change_id, changeset_id, patch_id
        )

        self._run_failpoint(failpoint, *arguments)

        _, interrupted = self._transaction_journal()
        self.assertEqual("prepared_apply", interrupted["phase"])
        self.assertEqual(patch_id, interrupted["patch_id"])
        self.assertEqual([], self._receipt_payloads("patch-applies"))
        self.assertEqual([], self._receipt_payloads("patch-undos"))
        expected_interrupted = (
            invalid_candidate if failpoint == "apply_after_prepared" else expected_applied
        )
        self.assertEqual(expected_interrupted, dataset.read_bytes())

        recovered = self.run_cli(*arguments).payload["artifact"]
        self.assertEqual(expected_outcome, recovered["outcome"])
        self.assertEqual(expected_applied, dataset.read_bytes())
        self.assertEqual(interrupted["undo_id"], recovered["undo_id"])
        self.assertEqual(interrupted["apply_id"], recovered["apply_id"])
        self.assertEqual(interrupted["applied_state_id"], recovered["applied_state_id"])

        replayed = self.run_cli(*arguments).payload["artifact"]
        self.assertEqual("already_applied", replayed["outcome"])
        self.assertEqual(recovered["apply_id"], replayed["apply_id"])
        self.assertEqual(recovered["undo_id"], replayed["undo_id"])

        _, completed = self._transaction_journal()
        self.assertEqual("applied", completed["phase"])
        apply_receipts = self._receipt_payloads("patch-applies")
        self.assertEqual(1, len(apply_receipts))
        expected_receipt = dict(recovered)
        expected_receipt["outcome"] = "applied"
        self.assertEqual(expected_receipt, apply_receipts[0])
        self.assertEqual([], self._receipt_payloads("patch-undos"))
        self._assert_staged_status(
            change_id,
            recovered["changeset_id"],
            original_head,
            recovered["applied_state_id"],
        )
        self._assert_no_patch_staging_leaks()

    def _assert_undo_recovery(self, failpoint: str, expected_outcome: str) -> None:
        (
            dataset,
            invalid_candidate,
            change_id,
            changeset_id,
            report,
            request,
            patch_id,
        ) = self._prepare_patch()
        original_head = self.status()["head_revision_id"]
        applied = self.run_cli(
            *self._apply_arguments(request, report, change_id, changeset_id, patch_id)
        ).payload["artifact"]
        applied_bytes = dataset.read_bytes()
        undo_arguments = ("patch-undo", applied["undo_id"], "--state", self.state)

        self._run_failpoint(failpoint, *undo_arguments)

        _, interrupted = self._transaction_journal()
        self.assertEqual("prepared_undo", interrupted["phase"])
        self.assertEqual(applied["apply_id"], interrupted["apply_id"])
        apply_receipts = self._receipt_payloads("patch-applies")
        self.assertEqual(1, len(apply_receipts))
        self.assertEqual([], self._receipt_payloads("patch-undos"))
        expected_interrupted = (
            applied_bytes if failpoint == "undo_after_prepared" else invalid_candidate
        )
        self.assertEqual(expected_interrupted, dataset.read_bytes())

        recovered = self.run_cli(*undo_arguments).payload["artifact"]
        self.assertEqual(expected_outcome, recovered["outcome"])
        self.assertEqual(invalid_candidate, dataset.read_bytes())
        self.assertEqual(applied["apply_id"], recovered["apply_id"])
        self.assertEqual(applied["undo_id"], recovered["undo_id"])

        replayed = self.run_cli(*undo_arguments).payload["artifact"]
        self.assertEqual("already_undone", replayed["outcome"])
        self.assertEqual(recovered["undo_receipt_id"], replayed["undo_receipt_id"])

        _, completed = self._transaction_journal()
        self.assertEqual("undone", completed["phase"])
        undo_receipts = self._receipt_payloads("patch-undos")
        self.assertEqual(1, len(undo_receipts))
        expected_receipt = dict(recovered)
        expected_receipt["outcome"] = "undone"
        self.assertEqual(expected_receipt, undo_receipts[0])
        self._assert_staged_status(
            change_id,
            changeset_id,
            original_head,
            recovered["restored_state_id"],
        )
        self._assert_no_patch_staging_leaks()

    def test_guarded_patch_apply_rejects_hardlinks_then_applies_and_undoes_exactly(self) -> None:
        (
            dataset,
            invalid_candidate,
            change_id,
            changeset_id,
            report,
            request,
            patch_id,
        ) = self._prepare_patch()
        wrong_patch_id = f"patch_{'0' * 64}"
        self.assert_cli_error(
            "PATCH_NOT_AUTHORIZED",
            *self._apply_arguments(
                request, report, change_id, changeset_id, wrong_patch_id
            ),
        )
        self.assertEqual(invalid_candidate, dataset.read_bytes())

        alias = self.root / "dataset-hardlink.jsonl"
        os.link(dataset, alias)
        self.assert_cli_error(
            "INVALID_ARGUMENT",
            *self._apply_arguments(request, report, change_id, changeset_id, patch_id),
        )
        self.assertEqual(invalid_candidate, dataset.read_bytes())
        alias.unlink()

        applied = self.run_cli(
            *self._apply_arguments(request, report, change_id, changeset_id, patch_id)
        ).payload["artifact"]
        self.assertEqual("applied", applied["outcome"])
        self.assertEqual([{"id": "alpha", "score": 0.75}], self._read_records(dataset))

        undone = self.run_cli(
            "patch-undo", applied["undo_id"], "--state", self.state
        ).payload["artifact"]
        self.assertEqual("undone", undone["outcome"])
        self.assertEqual(applied["apply_id"], undone["apply_id"])
        self.assertEqual(invalid_candidate, dataset.read_bytes())
        self.assert_no_staging_artifacts()

    def test_patch_apply_recovers_after_prepared_failpoint(self) -> None:
        self._assert_apply_recovery("apply_after_prepared", "applied")

    def test_patch_apply_recovers_after_rename_failpoint(self) -> None:
        self._assert_apply_recovery("apply_after_rename", "already_applied")

    def test_patch_undo_recovers_after_prepared_failpoint(self) -> None:
        self._assert_undo_recovery("undo_after_prepared", "undone")

    def test_patch_undo_recovers_after_rename_failpoint(self) -> None:
        self._assert_undo_recovery("undo_after_rename", "already_undone")

    def _read_records(self, dataset: Path) -> list[dict[str, Any]]:
        return [json.loads(line) for line in dataset.read_text().splitlines()]


if __name__ == "__main__":
    import unittest

    unittest.main()
