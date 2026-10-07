from __future__ import annotations

import json

from tests.cli_harness import DataJigCliTestCase


class EndToEndJourneyTests(DataJigCliTestCase):
    def test_three_day_logs_reach_versioned_and_pipeline_training_deliveries(self) -> None:
        raw = self.root / "logs"
        raw.mkdir()
        for day in range(1, 4):
            rows = ["event_id,user_id,day,action,price"]
            for offset in range(120):
                index = (day - 1) * 120 + offset
                rows.append(
                    f"event-{index:03d},user-{index % 162:03d},{day},"
                    f"{'buy' if index % 4 == 0 else 'view'},{index % 31 + 1}"
                )
            (raw / f"day{day}.csv").write_text("\n".join(rows) + "\n", encoding="utf-8")

        recipe = self.root / "prepare-recipe.json"
        recipe.write_text(
            json.dumps(
                {
                    "namespace": "datajig",
                    "kind": "prepare",
                    "schema_version": 1,
                    "source": {"format": "csv", "include": ["day*.csv"]},
                    "output": {"format": "jsonl"},
                    "id_field": "event_id",
                    "steps": [
                        {"op": "cast", "field": "day", "type": "integer"},
                        {"op": "cast", "field": "price", "type": "integer"},
                    ],
                }
            ),
            encoding="utf-8",
        )
        prepared = self.root / "prepared" / "events.jsonl"
        prepare_plan = self.root / "artifacts" / "prepare-plan.json"
        planned_prepare = self.run_cli(
            "prepare-plan",
            raw,
            "--recipe",
            recipe,
            "--output",
            prepared,
            "--plan",
            prepare_plan,
        ).payload["artifact"]
        self.assertEqual(360, planned_prepare["source_rows"])
        self.run_cli(
            "prepare-apply",
            prepare_plan,
            "--accept-plan",
            planned_prepare["plan_id"],
        )
        self.assertEqual(360, len(prepared.read_text(encoding="utf-8").splitlines()))

        features = self.root / "prepared" / "training.jsonl"
        transform_plan = self.root / "artifacts" / "transform-plan.json"
        sql = (
            "SELECT user_id AS id, COUNT(*) AS events, CAST(SUM(price) AS BIGINT) AS spend, "
            "CAST(SUM(CASE WHEN action = 'buy' THEN 1 ELSE 0 END) AS BIGINT) AS purchases, "
            "MAX(day) AS last_day, AVG(price) AS average_price "
            "FROM prepared GROUP BY user_id ORDER BY id"
        )
        planned_transform = self.run_cli(
            "transform-plan",
            "--input",
            f"prepared={prepared}",
            "--sql",
            sql,
            "--id-field",
            "id",
            "--output",
            features,
            "--plan",
            transform_plan,
        ).payload["artifact"]
        transformed = self.run_cli(
            "transform-apply",
            transform_plan,
            "--accept-plan",
            planned_transform["plan_id"],
        ).payload["artifact"]
        self.assertEqual(162, planned_transform["rows"])
        transform_receipt = transformed["receipt"]

        state = self.root / "workspace" / ".datajig"
        initialized = self.run_cli(
            "init",
            features,
            "--id-field",
            "id",
            "--source-receipt",
            transform_receipt,
            "--state",
            state,
        ).payload["artifact"]
        initial_revision = initialized["head_revision_id"]
        declared = self.run_cli(
            "changeset-begin",
            "--state",
            state,
            "--intent",
            "Record reviewed training features",
            "--task-id",
            "journey-review",
        ).payload["artifact"]
        records = [json.loads(line) for line in features.read_text(encoding="utf-8").splitlines()]
        for record in records:
            record["reviewed"] = True
        self.write_jsonl(features, records)
        staged = self.run_cli(
            "changeset-stage", "--state", state, "--change", declared["change_id"]
        ).payload["artifact"]
        review = self.run_cli(
            "check",
            "--state",
            state,
            "--change",
            declared["change_id"],
            "--changeset",
            staged["changeset_id"],
        ).payload["artifact"]
        sealed = self.run_cli(
            "seal",
            "--state",
            state,
            "--change",
            declared["change_id"],
            "--changeset",
            staged["changeset_id"],
            "--accept-report",
            review["report_content_id"],
            "--message",
            "Approve reviewed training features",
        ).payload["artifact"]
        self.assertNotEqual(initial_revision, sealed["revision_id"])

        bundle = self.root / "manual-delivery" / "bundle"
        exported = self.run_cli(
            "export",
            "--state",
            state,
            "--output",
            bundle,
            "--split",
            "train=7",
            "--split",
            "val=2",
            "--split",
            "test=1",
        ).payload["artifact"]
        manifest = json.loads(
            (bundle / "datajig.bundle.json").read_text(encoding="utf-8")
        )
        self.assertEqual(162, sum(item["records"] for item in manifest["splits"].values()))
        consume_plan = self.root / "manual-delivery" / "pytorch.consume.json"
        consumption = self.run_cli(
            "consume-plan",
            exported["manifest"],
            "--split",
            "train",
            "--consumer",
            "pytorch",
            "--run-id",
            "run-journey-001",
            "--output",
            self.root / "manual-delivery" / "run",
            "--plan",
            consume_plan,
        ).payload["artifact"]
        self.assertTrue(consumption["consumption_plan_id"].startswith("consume_"))

        config = self.root / "pipeline.json"
        config.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "pipeline": {"name": "journey", "provider": "duckdb"},
                    "target": {
                        "dataset": "pipeline/training.jsonl",
                        "state": "pipeline/.datajig",
                        "mode": "create",
                    },
                    "delivery": {"output": "pipeline/delivery"},
                    "inputs": [
                        {"alias": "prepared", "path": "prepared/events.jsonl", "format": "jsonl"}
                    ],
                    "transform": {"sql": sql, "id_field": "id", "params": []},
                    "export": {"split": ["train=7", "val=2", "test=1"]},
                    "consumption_plan": [
                        {"consumer": "pytorch", "split": "train", "run_id": "run-journey-001"}
                    ],
                }
            ),
            encoding="utf-8",
        )
        pipeline_plan = self.root / "artifacts" / "pipeline-plan.json"
        planned_pipeline = self.run_cli(
            "pipeline", "plan", "--config", config, "--plan", pipeline_plan
        ).payload["artifact"]
        receipt = self.run_cli(
            "pipeline",
            "apply",
            pipeline_plan,
            "--accept-plan",
            planned_pipeline["pipeline_id"],
        ).payload["artifact"]
        receipt_path = self.root / "pipeline" / "delivery" / "pipeline-receipt.json"
        lineage = self.run_cli("lineage", receipt_path).payload["artifact"]
        self.assertEqual("committed", receipt["status"])
        self.assertEqual(
            ["source", "transform", "revision", "bundle", "consumption_plan"],
            [node["kind"] for node in lineage["nodes"]],
        )
        self.assertTrue(
            (self.root / "pipeline" / "delivery" / ".datajig-commit.json").is_file()
        )


if __name__ == "__main__":
    import unittest

    unittest.main()
