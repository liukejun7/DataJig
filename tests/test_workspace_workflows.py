from __future__ import annotations

import fcntl
import os

from tests.cli_harness import DataJigCliTestCase


class WorkspaceWorkflowTests(DataJigCliTestCase):
    def test_unique_active_changeset_aliases_avoid_copying_content_ids(self) -> None:
        dataset, _ = self.initialize_jsonl([{"id": "alpha", "score": 1}])
        change_id, _ = self.begin_change("alias-resolution")
        self.write_jsonl(dataset, [{"id": "alpha", "score": 2}])
        changeset_id, _ = self.stage_change(change_id)

        checked = self.run_cli(
            "check",
            "--state",
            self.state,
            "--change",
            "@active",
            "--changeset",
            "@latest",
        ).payload

        self.assertEqual(change_id, checked["artifact"]["change_id"])
        self.assertEqual(changeset_id, checked["artifact"]["changeset_id"])

    def test_unique_active_changeset_needs_no_ids_through_seal(self) -> None:
        dataset, _ = self.initialize_jsonl([{"id": "alpha", "score": 1}])
        change_id, _ = self.begin_change("missing-binding")
        self.write_jsonl(dataset, [{"id": "alpha", "score": 2}])
        changeset_id, _ = self.stage_change(change_id)

        checked = self.run_cli("check", "--state", self.state).payload
        review = checked["artifact"]
        self.assertEqual(change_id, review["change_id"])
        self.assertEqual(changeset_id, review["changeset_id"])
        self.assertEqual(
            [
                "--state",
                str(self.state),
                "--change",
                change_id,
                "--changeset",
                changeset_id,
                "--accept-report",
                review["report_content_id"],
            ],
            checked["next_actions"][-1]["args"],
        )

        status_result = self.run_cli("status", "--state", self.state).payload
        status = status_result["artifact"]
        self.assertEqual(change_id, status["change_id"])
        self.assertEqual(changeset_id, status["changeset_id"])
        self.assertEqual(
            [
                "--state",
                str(self.state),
                "--change",
                change_id,
                "--changeset",
                changeset_id,
            ],
            status_result["next_actions"][0]["args"],
        )

        planned = self.run_cli("plan", "--state", self.state).payload
        self.assertEqual(review["report_content_id"], planned["artifact"]["report_content_id"])
        self.assertEqual(
            [
                "--state",
                str(self.state),
                "--change",
                change_id,
                "--changeset",
                changeset_id,
                "--accept-report",
                review["report_content_id"],
            ],
            planned["next_actions"][-1]["args"],
        )

        sealed = self.run_cli(
            "seal",
            "--state",
            self.state,
            "--accept-report",
            review["report_content_id"],
            "--message",
            "Accept automatically resolved candidate",
        ).payload["artifact"]
        self.assertEqual(change_id, sealed["change_id"])
        self.assertEqual(changeset_id, sealed["changeset_id"])

    def test_automatic_context_fails_closed_when_no_candidate_exists(self) -> None:
        self.initialize_jsonl([{"id": "alpha", "score": 1}])

        failed = self.assert_cli_error("INVALID_ARGUMENT", "check", "--state", self.state)

        self.assertIn(
            "found 0 compatible active change declarations",
            failed.payload["error"]["message"],
        )

    def test_automatic_context_fails_closed_when_candidates_are_ambiguous(self) -> None:
        dataset, _ = self.initialize_jsonl([{"id": "alpha", "score": 1}])
        self.begin_change("first-candidate")
        self.begin_change("second-candidate")
        self.write_jsonl(dataset, [{"id": "alpha", "score": 2}])

        failed = self.assert_cli_error("INVALID_ARGUMENT", "check", "--state", self.state)

        self.assertIn(
            "found 2 compatible active change declarations",
            failed.payload["error"]["message"],
        )

        status_failed = self.assert_cli_error(
            "INVALID_ARGUMENT", "status", "--state", self.state
        )
        self.assertIn(
            "found 2 compatible active change declarations",
            status_failed.payload["error"]["message"],
        )

    def test_failed_check_returns_plan_and_bounded_findings_actions(self) -> None:
        policy = self.root / "quality.json"
        policy.write_text(
            '{"namespace":"datajig","schema_version":1,"adapter":"jsonl",'
            '"mode":"changed_only","fields":{"score":{"types":["number"],'
            '"maximum":1}}}',
            encoding="utf-8",
        )
        dataset, _ = self.initialize_jsonl([{"id": "alpha", "score": 1}], policy=policy)
        change_id, _ = self.begin_change("repair-actions")
        self.write_jsonl(dataset, [{"id": "alpha", "score": 2}])
        self.stage_change(change_id)

        checked = self.run_cli("check", "--state", self.state).payload

        self.assertEqual(
            ["plan", "findings"],
            [action["command"] for action in checked["next_actions"]],
        )
        self.assertEqual(
            [
                "--state",
                str(self.state),
                "--change",
                change_id,
                "--changeset",
                checked["artifact"]["changeset_id"],
            ],
            checked["next_actions"][0]["args"],
        )
        self.assertEqual(
            [checked["artifact"]["report_path"], "--offset", "0", "--limit", "50"],
            checked["next_actions"][1]["args"],
        )

    def test_changeset_check_and_seal_advance_head_once_and_leave_clean_status(self) -> None:
        dataset, initialized = self.initialize_jsonl(
            [{"id": "alpha", "score": 1}, {"id": "beta", "score": 2}]
        )
        original_head = initialized["head_revision_id"]
        change_id, _ = self.begin_change()
        self.write_jsonl(
            dataset,
            [{"id": "alpha", "score": 3}, {"id": "beta", "score": 2}],
        )
        changeset_id, _ = self.stage_change(change_id)

        review = self.check_change(change_id, changeset_id)
        self.assertEqual("seal", review["decision"])
        sealed = self.run_cli(
            "seal",
            "--state",
            self.state,
            "--change",
            change_id,
            "--changeset",
            changeset_id,
            "--accept-report",
            review["report_content_id"],
            "--message",
            "Accept regression candidate",
        ).payload["artifact"]

        self.assertEqual(original_head, sealed["parent_revision_id"])
        self.assertNotEqual(original_head, sealed["revision_id"])
        status = self.status()
        self.assertEqual(sealed["revision_id"], status["head_revision_id"])
        self.assertTrue(status["clean"])
        self.assertEqual(0, status["changed_files"])
        history = self.run_cli("log", "--state", self.state).payload["artifact"]
        self.assertEqual(2, history["returned"])
        self.assertEqual(sealed["revision_id"], history["revisions"][0]["revision_id"])
        self.assertEqual(original_head, history["revisions"][1]["revision_id"])
        self.assert_no_staging_artifacts()

    def test_unstaged_live_edit_is_rejected_without_advancing_head(self) -> None:
        dataset, initialized = self.initialize_jsonl([{"id": "alpha", "score": 1}])
        original_head = initialized["head_revision_id"]
        change_id, _ = self.begin_change()
        self.write_jsonl(dataset, [{"id": "alpha", "score": 2}])
        stale_changeset_id, _ = self.stage_change(change_id)
        self.write_jsonl(dataset, [{"id": "alpha", "score": 3}])

        self.assert_cli_error(
            "UNSTAGED_CHANGES",
            "check",
            "--state",
            self.state,
            "--change",
            change_id,
            "--changeset",
            stale_changeset_id,
        )
        status = self.status()
        self.assertEqual(original_head, status["head_revision_id"])
        self.assertFalse(status["clean"])
        self.assert_no_staging_artifacts()

    def test_competing_changeset_cannot_use_stale_base_after_head_advances(self) -> None:
        dataset, initialized = self.initialize_jsonl([{"id": "alpha", "score": 1}])
        original_head = initialized["head_revision_id"]
        winner_change, winner_declaration = self.begin_change("winner")
        stale_change, stale_declaration = self.begin_change("stale-competitor")
        self.assertEqual(original_head, winner_declaration["base_revision_id"])
        self.assertEqual(original_head, stale_declaration["base_revision_id"])

        winner_bytes = self.write_jsonl(dataset, [{"id": "alpha", "score": 2}])
        winner_changeset, winner_stage = self.stage_change(winner_change)
        self.write_jsonl(dataset, [{"id": "alpha", "score": 3}])
        stale_changeset, stale_stage = self.stage_change(stale_change)
        self.assertEqual(original_head, winner_stage["base_revision_id"])
        self.assertEqual(original_head, stale_stage["base_revision_id"])

        dataset.write_bytes(winner_bytes)
        winner_review = self.check_change(winner_change, winner_changeset)
        sealed = self.run_cli(
            "seal",
            "--state",
            self.state,
            "--change",
            winner_change,
            "--changeset",
            winner_changeset,
            "--accept-report",
            winner_review["report_content_id"],
            "--message",
            "Advance competing HEAD",
        ).payload["artifact"]
        advanced_head = sealed["revision_id"]
        self.assertNotEqual(original_head, advanced_head)

        stale_check = self.assert_cli_error(
            "WORKSPACE_ERROR",
            "check",
            "--state",
            self.state,
            "--change",
            stale_change,
            "--changeset",
            stale_changeset,
        )
        self.assertFalse(stale_check.payload["error"]["retryable"])
        stale_seal = self.assert_cli_error(
            "WORKSPACE_ERROR",
            "seal",
            "--state",
            self.state,
            "--change",
            stale_change,
            "--changeset",
            stale_changeset,
            "--accept-report",
            winner_review["report_content_id"],
            "--message",
            "Must not replace advanced HEAD",
        )
        self.assertFalse(stale_seal.payload["error"]["retryable"])

        self.assertEqual(winner_bytes, dataset.read_bytes())
        status = self.status()
        self.assertEqual(advanced_head, status["head_revision_id"])
        self.assertTrue(status["clean"])
        history = self.run_cli("log", "--state", self.state).payload["artifact"]
        self.assertEqual(2, history["returned"])
        self.assertEqual(advanced_head, history["revisions"][0]["revision_id"])
        self.assertEqual(original_head, history["revisions"][1]["revision_id"])
        self.assert_no_staging_artifacts()

    def test_writer_reports_workspace_busy_while_os_lock_is_held(self) -> None:
        dataset, initialized = self.initialize_jsonl([{"id": "alpha", "score": 1}])
        original_head = initialized["head_revision_id"]
        original_data = dataset.read_bytes()
        refs = self.state / "refs.json"
        original_refs = refs.read_bytes()
        lock_path = self.state / ".workspace.lock"

        with lock_path.open("a+b") as lock_file:
            os.chmod(lock_path, 0o600)
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                blocked = self.assert_cli_error(
                    "WORKSPACE_BUSY",
                    "changeset-begin",
                    "--state",
                    self.state,
                    "--intent",
                    "Blocked writer must not publish",
                    "--task-id",
                    "lock-conflict",
                )
                self.assertTrue(blocked.payload["error"]["retryable"])
                self.assertEqual(original_data, dataset.read_bytes())
                self.assertEqual(original_refs, refs.read_bytes())
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

        declaration = self.run_cli(
            "changeset-begin",
            "--state",
            self.state,
            "--intent",
            "Proceed after lock release",
            "--task-id",
            "lock-conflict",
        ).payload["artifact"]
        self.assertEqual(original_head, declaration["base_revision_id"])
        self.assertEqual(original_data, dataset.read_bytes())
        self.assertEqual(original_refs, refs.read_bytes())
        status = self.status()
        self.assertEqual(original_head, status["head_revision_id"])
        self.assertTrue(status["clean"])
        self.assert_no_staging_artifacts()


if __name__ == "__main__":
    import unittest

    unittest.main()
