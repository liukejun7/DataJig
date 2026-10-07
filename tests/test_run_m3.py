from __future__ import annotations

import json

from tests.cli_harness import DataJigCliTestCase


class RunM3EvidenceTests(DataJigCliTestCase):
    def test_success_persists_one_identity_bound_run_receipt(self) -> None:
        source = self.root / "rows.csv"
        source.write_text("id,value\na,1\nb,2\n", encoding="utf-8")

        artifact = self.run_cli(
            "run",
            "from rows.csv source-id-field id export id-field id",
            "--output",
            "delivery",
        ).payload["artifact"]

        receipt_path = (
            self.root
            / ".datajig"
            / "runs"
            / artifact["intent_id"]
            / "receipts"
            / f"{artifact['attempt_id']}.json"
        )
        self.assertTrue(receipt_path.is_file())
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(1, receipt["run_receipt_schema_version"])
        self.assertEqual("committed", receipt["status"])
        self.assertEqual(artifact["intent_id"], receipt["intent_id"])
        self.assertEqual(artifact["plan_id"], receipt["plan_id"])
        self.assertEqual(artifact["attempt_id"], receipt["attempt_id"])
        self.assertEqual(artifact["pipeline_id"], receipt["pipeline_id"])
        self.assertEqual(artifact["pipeline_receipt_id"], receipt["pipeline_receipt_id"])
        self.assertEqual(artifact["final_revision"], receipt["revision_id"])
        self.assertEqual(artifact["bundle_id"], receipt["bundle_id"])
        self.assertEqual(artifact["run_receipt_id"], receipt["run_receipt_id"])
        self.assertTrue(receipt["run_receipt_id"].startswith("runrcpt_"))

    def test_recovery_publishes_the_receipt_once_and_reopens_idempotently(self) -> None:
        source = self.root / "rows.csv"
        source.write_text("id,value\na,1\nb,2\n", encoding="utf-8")
        planned = self.run_cli(
            "run",
            "from rows.csv source-id-field id export id-field id",
            "--strict",
            "--output",
            "delivery",
        ).payload["artifact"]
        resume = (
            "run",
            "--resume",
            planned["attempt_id"],
            "--accept-plan",
            planned["plan_id"],
        )

        self.run_cli(
            *resume,
            expected_returncode=2,
            extra_env={"DATAJIG_PIPELINE_FAILPOINT": "after_head_commit"},
        )
        receipt_path = (
            self.root
            / ".datajig"
            / "runs"
            / planned["intent_id"]
            / "receipts"
            / f"{planned['attempt_id']}.json"
        )
        self.assertFalse(receipt_path.exists())

        recovered = self.run_cli(*resume).payload["artifact"]
        first_bytes = receipt_path.read_bytes()
        reopened = self.run_cli(*resume, "--yes").payload["artifact"]

        self.assertEqual(recovered["run_receipt_id"], reopened["run_receipt_id"])
        self.assertEqual(first_bytes, receipt_path.read_bytes())
        self.assertEqual(1, len(list(receipt_path.parent.glob("*.json"))))

    def test_recovery_refuses_a_tampered_run_receipt(self) -> None:
        source = self.root / "rows.csv"
        source.write_text("id,value\na,1\nb,2\n", encoding="utf-8")
        completed = self.run_cli(
            "run",
            "from rows.csv source-id-field id export id-field id",
            "--output",
            "delivery",
        ).payload["artifact"]
        receipt_path = self.root / completed["run_receipt_path"]
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt["bundle_id"] = f"bundle_{'f' * 64}"
        receipt_path.write_text(json.dumps(receipt) + "\n", encoding="utf-8")

        result = self.run_cli(
            "run",
            "--resume",
            completed["attempt_id"],
            "--accept-plan",
            completed["plan_id"],
            "--yes",
            expected_returncode=2,
        )

        self.assertEqual("RUN_RECEIPT_CONFLICT", result.payload["error"]["code"])


if __name__ == "__main__":
    import unittest

    unittest.main()
