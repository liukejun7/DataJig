from __future__ import annotations

import json

from tests.test_pipeline_apply import PipelineApplyTests


class PipelineObservabilityTests(PipelineApplyTests):
    def test_info_and_lineage_verify_committed_receipt(self) -> None:
        config, _ = self._project()
        plan, planned = self._plan(config)
        receipt = self.run_cli(
            "pipeline", "apply", plan, "--accept-plan", planned["pipeline_id"]
        ).payload["artifact"]
        receipt_path = config.parent / "deliveries" / "user-agg-train" / "pipeline-receipt.json"

        info = self.run_cli("pipeline", "info", receipt_path, "--verify").payload["artifact"]
        self.assertEqual(receipt["pipeline_receipt_id"], info["pipeline_receipt_id"])
        markdown = config.parent / "pipeline.md"
        self.run_cli(
            "pipeline",
            "info",
            receipt_path,
            "--verify",
            "--format",
            "markdown",
            "--output",
            markdown,
        )
        rendered = markdown.read_text(encoding="utf-8")
        self.assertIn(receipt["pipeline_receipt_id"], rendered)
        self.assertIn(receipt["final_revision"], rendered)

        lineage = self.run_cli("lineage", receipt_path, "--format", "json").payload["artifact"]
        self.assertEqual(receipt["pipeline_id"], lineage["pipeline_id"])
        self.assertEqual(
            ["source", "transform", "revision", "bundle", "consumption_plan"],
            [node["kind"] for node in lineage["nodes"]],
        )
        self.assertEqual(
            [planned["inputs"][0]["content_id"]],
            lineage["nodes"][0]["ids"],
        )

        state = config.parent / "workspace" / ".datajig"
        for artifact_id in (
            receipt["final_revision"],
            receipt["bundle_id"],
            receipt["consumption_plans"][0]["plan_id"],
        ):
            with self.subTest(artifact_id=artifact_id):
                resolved = self.run_cli(
                    "lineage", artifact_id, "--state", state, "--format", "json"
                ).payload["artifact"]
                self.assertEqual(receipt["pipeline_id"], resolved["pipeline_id"])

    def test_lineage_resolves_pipeline_id_from_state(self) -> None:
        config, _ = self._project()
        plan, planned = self._plan(config)
        self.run_cli("pipeline", "apply", plan, "--accept-plan", planned["pipeline_id"])
        state = config.parent / "workspace" / ".datajig"
        lineage = self.run_cli(
            "lineage", planned["pipeline_id"], "--state", state, "--format", "text"
        )
        self.assertIn(
            "source -> transform -> revision -> bundle -> consumption_plan",
            lineage.payload["artifact"]["text"],
        )
        status = self.run_cli("status", "--state", state).payload["artifact"]
        self.assertEqual(planned["pipeline_id"], status["recent_pipeline"]["pipeline_id"])

    def test_gc_lists_and_removes_only_pipeline_attempts(self) -> None:
        config, _ = self._project()
        plan, planned = self._plan(config)
        self.run_cli(
            "pipeline",
            "apply",
            plan,
            "--accept-plan",
            planned["pipeline_id"],
            expected_returncode=2,
            extra_env={"DATAJIG_PIPELINE_FAILPOINT": "after_head_commit"},
        )
        state = config.parent / "workspace" / ".datajig"
        dry_run = self.run_cli("pipeline", "gc", "--state", state, "--dry-run").payload["artifact"]
        self.assertGreaterEqual(dry_run["recoverable_attempts"], 1)
        self.assertEqual(0, dry_run["removed_attempts"])

        blocked = self.assert_cli_error(
            "PIPELINE_GC_REQUIRES_RESUME", "pipeline", "gc", "--state", state
        )
        self.assertIn("--resume", blocked.payload["error"]["remediation"])

    def test_tampered_receipt_is_rejected_by_info_and_lineage(self) -> None:
        config, _ = self._project()
        plan, planned = self._plan(config)
        self.run_cli("pipeline", "apply", plan, "--accept-plan", planned["pipeline_id"])
        receipt_path = config.parent / "deliveries" / "user-agg-train" / "pipeline-receipt.json"
        payload = json.loads(receipt_path.read_text(encoding="utf-8"))
        payload["bundle_id"] = "bundle_" + "0" * 64
        receipt_path.write_text(json.dumps(payload), encoding="utf-8")

        self.assert_cli_error(
            "PIPELINE_RECEIPT_TAMPERED", "pipeline", "info", receipt_path, "--verify"
        )
        self.assert_cli_error("PIPELINE_RECEIPT_TAMPERED", "lineage", receipt_path)
