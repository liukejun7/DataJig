from __future__ import annotations

import json

from tests.cli_harness import DataJigCliTestCase


class RunM4ObservabilityTests(DataJigCliTestCase):
    def test_run_receipt_exposes_the_complete_identity_lineage(self) -> None:
        source = self.root / "rows.csv"
        source.write_text("id,value\na,1\nb,2\n", encoding="utf-8")
        completed = self.run_cli(
            "run",
            "from rows.csv source-id-field id export id-field id",
            "--output",
            "delivery",
            "--consume",
            "--consumer",
            "pytorch",
            "--run-id",
            "training-001",
            "--run-dir",
            "runs/training-001",
        ).payload["artifact"]

        lineage = self.run_cli(
            "lineage", completed["run_receipt_path"], "--format", "json"
        ).payload["artifact"]

        self.assertTrue(lineage["complete"])
        self.assertEqual(completed["run_receipt_id"], lineage["run_receipt_id"])
        self.assertEqual(
            [
                "source",
                "intent",
                "plan",
                "attempt",
                "revision",
                "bundle",
                "consumption",
                "run_receipt",
            ],
            [node["kind"] for node in lineage["nodes"]],
        )
        self.assertEqual(completed["intent_id"], lineage["nodes"][1]["id"])
        self.assertEqual(completed["plan_id"], lineage["nodes"][2]["id"])
        self.assertEqual(completed["attempt_id"], lineage["nodes"][3]["id"])
        self.assertEqual(completed["final_revision"], lineage["nodes"][4]["id"])
        self.assertEqual(completed["bundle_id"], lineage["nodes"][5]["id"])
        self.assertEqual(
            [completed["consumption_plans"][0]["plan_id"]],
            lineage["nodes"][6]["ids"],
        )
        resolved = self.run_cli(
            "lineage",
            completed["run_receipt_id"],
            "--state",
            self.root / ".datajig",
        ).payload["artifact"]
        self.assertEqual(completed["run_receipt_id"], resolved["run_receipt_id"])

    def test_workspace_status_reports_the_recent_run(self) -> None:
        source = self.root / "rows.csv"
        source.write_text("id,value\na,1\nb,2\n", encoding="utf-8")
        completed = self.run_cli(
            "run",
            "from rows.csv source-id-field id export id-field id",
            "--output",
            "delivery",
        ).payload["artifact"]
        state = (
            self.root
            / ".datajig"
            / "runs"
            / completed["intent_id"]
            / "workspace"
            / "state"
        )

        status = self.run_cli("status", "--state", state).payload["artifact"]

        self.assertEqual(completed["run_receipt_id"], status["recent_run"]["run_receipt_id"])
        self.assertEqual(completed["attempt_id"], status["recent_run"]["attempt_id"])
        self.assertEqual([], status["recoverable_runs"])
        self.assertEqual(0, status["pending_run_gc"])
        text_status = self.run_cli("status", "--state", state, "--format", "text").payload[
            "artifact"
        ]
        self.assertIn("DataJig workspace", text_status["text"])
        self.assertIn(completed["run_receipt_id"], text_status["text"])

    def test_status_finds_a_recoverable_attempt_and_repairs_a_corrupt_index(self) -> None:
        source = self.root / "rows.csv"
        source.write_text("id,value\na,1\nb,2\n", encoding="utf-8")
        task = "from rows.csv source-id-field id export id-field id"
        completed = self.run_cli("run", task, "--output", "delivery").payload["artifact"]
        index = self.root / ".datajig" / "meta" / "run-index.json"
        index.write_text("{broken", encoding="utf-8")

        planned = self.run_cli(
            "run", task, "--strict", "--output", "next-delivery"
        ).payload["artifact"]
        self.run_cli(
            "run",
            "--resume",
            planned["attempt_id"],
            "--accept-plan",
            planned["plan_id"],
            expected_returncode=2,
            extra_env={"DATAJIG_PIPELINE_FAILPOINT": "after_head_commit"},
        )
        state = (
            self.root
            / ".datajig"
            / "runs"
            / completed["intent_id"]
            / "workspace"
            / "state"
        )

        status = self.run_cli("status", "--state", state).payload["artifact"]

        self.assertEqual(completed["run_receipt_id"], status["recent_run"]["run_receipt_id"])
        self.assertEqual([planned["attempt_id"]], [
            item["attempt_id"] for item in status["recoverable_runs"]
        ])
        self.assertEqual(0, status["pending_run_gc"])
        self.assertEqual("run_index", json.loads(index.read_text())["kind"])
        stale = json.loads(index.read_text())
        stale["entries"] = []
        index.write_text(json.dumps(stale), encoding="utf-8")
        repaired = self.run_cli("status", "--state", state).payload["artifact"]
        self.assertEqual(completed["run_receipt_id"], repaired["recent_run"]["run_receipt_id"])


if __name__ == "__main__":
    import unittest

    unittest.main()
