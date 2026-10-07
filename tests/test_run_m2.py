from __future__ import annotations

import json
import os
from unittest.mock import patch

import duckdb

from datajig.pipeline import PipelineError
from datajig.run import execute_accepted_run
from tests.cli_harness import DataJigCliTestCase


class RunM2JourneyTests(DataJigCliTestCase):
    def test_csv_file_runs_through_pipeline_to_a_committed_bundle(self) -> None:
        source = self.root / "rows.csv"
        source.write_text(
            "name,value\n" + "".join(f"row-{index},{index}\n" for index in range(12)),
            encoding="utf-8",
        )

        result = self.run_cli("run", "整理 rows.csv", "--output", "delivery")

        self.assertEqual("run_completed", result.payload["kind"])
        self.assertEqual("committed", result.payload["artifact"]["status"])
        self.assertTrue(result.payload["artifact"]["pipeline_id"].startswith("pipe_"))
        self.assertTrue(result.payload["artifact"]["bundle_id"].startswith("bundle_"))
        delivery = self.root / "delivery"
        self.assertTrue((delivery / ".datajig-commit.json").is_file())
        self.assertTrue((delivery / "pipeline-receipt.json").is_file())
        manifest = json.loads((delivery / "bundle" / "datajig.bundle.json").read_text())
        self.assertEqual(result.payload["artifact"]["bundle_id"], manifest["bundle_id"])
        self.assertEqual(12, manifest["records"])

    def test_csv_directory_runs_to_a_bundle_with_stable_generated_ids(self) -> None:
        source = self.root / "days"
        source.mkdir()
        (source / "day-1.csv").write_text("name,value\na,1\nb,2\n", encoding="utf-8")
        (source / "day-2.csv").write_text("name,value\nc,3\nd,4\n", encoding="utf-8")

        result = self.run_cli("run", "整理 days", "--output", "directory-delivery")

        self.assertEqual("run_completed", result.payload["kind"])
        manifest = json.loads(
            (self.root / "directory-delivery/bundle/datajig.bundle.json").read_text()
        )
        self.assertEqual(4, manifest["records"])

    def test_jsonl_runs_to_a_bundle(self) -> None:
        source = self.root / "rows.jsonl"
        self.write_jsonl(source, [{"name": "a", "value": 1}, {"name": "b", "value": 2}])

        result = self.run_cli("run", "整理 rows.jsonl", "--output", "jsonl-delivery")

        self.assertEqual("run_completed", result.payload["kind"])
        manifest = json.loads(
            (self.root / "jsonl-delivery/bundle/datajig.bundle.json").read_text()
        )
        self.assertEqual(2, manifest["records"])

    def test_parquet_runs_to_a_bundle(self) -> None:
        source = self.root / "rows.parquet"
        connection = duckdb.connect()
        try:
            connection.execute(
                "COPY (SELECT * FROM (VALUES ('a', 1), ('b', 2)) AS rows(name, value)) "
                f"TO '{source.as_posix()}' (FORMAT PARQUET)"
            )
        finally:
            connection.close()

        result = self.run_cli("run", "整理 rows.parquet", "--output", "parquet-delivery")

        self.assertEqual("run_completed", result.payload["kind"])
        manifest = json.loads(
            (self.root / "parquet-delivery/bundle/datajig.bundle.json").read_text()
        )
        self.assertEqual(2, manifest["records"])

    def test_changed_source_advances_the_same_owned_delivery(self) -> None:
        source = self.root / "rows.csv"
        source.write_text("id,value\na,1\n", encoding="utf-8")
        task = "from rows.csv source-id-field id export id-field id"
        first = self.run_cli("run", task, "--output", "delivery").payload["artifact"]
        source.write_text("id,value\na,2\nb,3\n", encoding="utf-8")

        second = self.run_cli("run", task, "--output", "delivery", "--yes").payload[
            "artifact"
        ]

        self.assertNotEqual(first["final_revision"], second["final_revision"])
        self.assertNotEqual(first["pipeline_id"], second["pipeline_id"])
        backup = self.root / second["delivery_backup"]
        self.assertTrue((backup / ".datajig-commit.json").is_file())

    def test_consume_flag_adds_the_requested_training_binding(self) -> None:
        source = self.root / "rows.csv"
        source.write_text(
            "id,value\n" + "".join(f"row-{index},{index}\n" for index in range(100)),
            encoding="utf-8",
        )

        artifact = self.run_cli(
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

        self.assertEqual("pytorch", artifact["consumption_plans"][0]["consumer"])
        self.assertEqual("training-001", artifact["consumption_plans"][0]["run_id"])
        self.assertTrue(artifact["consumption_plans"][0]["plan_id"].startswith("consume_"))

    def test_strict_run_stops_at_the_plan_boundary(self) -> None:
        source = self.root / "rows.csv"
        source.write_text("id,value\na,1\n", encoding="utf-8")

        result = self.run_cli(
            "run",
            "from rows.csv source-id-field id export id-field id",
            "--strict",
            "--output",
            "delivery",
        )

        self.assertEqual("run_plan_ready", result.payload["kind"])
        self.assertFalse((self.root / "delivery").exists())

        artifact = result.payload["artifact"]
        self.assertTrue(
            artifact["binding"]["provider_identity"]["provider_id"].startswith("provider_")
        )
        resumed = self.run_cli(
            "run",
            "--resume",
            artifact["attempt_id"],
            "--accept-plan",
            artifact["plan_id"],
        )
        self.assertEqual("run_completed", resumed.payload["kind"])
        self.assertTrue((self.root / "delivery/.datajig-commit.json").is_file())

    def test_strict_plan_rechecks_update_authorization_when_delivery_appears(self) -> None:
        source = self.root / "rows.csv"
        source.write_text("id,value\na,1\n", encoding="utf-8")
        task = "from rows.csv source-id-field id export id-field id"
        planned = self.run_cli(
            "run", task, "--strict", "--output", "delivery"
        ).payload["artifact"]
        self.run_cli("run", task, "--output", "delivery")

        resumed = self.run_cli(
            "run",
            "--resume",
            planned["attempt_id"],
            "--accept-plan",
            planned["plan_id"],
        ).payload

        self.assertEqual("run_plan_ready", resumed["kind"])
        self.assertEqual("confirmation_required", resumed["decision"])
        self.assertIn("--yes", resumed["next_actions"][0]["args"])

    def test_run_resume_recovers_an_interrupted_pipeline(self) -> None:
        source = self.root / "rows.csv"
        source.write_text("id,value\na,1\nb,2\n", encoding="utf-8")
        task = "from rows.csv source-id-field id export id-field id"
        planned = self.run_cli(
            "run", task, "--strict", "--output", "delivery"
        ).payload["artifact"]
        resume = (
            "run",
            "--resume",
            planned["attempt_id"],
            "--accept-plan",
            planned["plan_id"],
        )
        interrupted = self.run_cli(
            *resume,
            expected_returncode=2,
            extra_env={"DATAJIG_PIPELINE_FAILPOINT": "after_head_commit"},
        )
        self.assertEqual("PIPELINE_INTERRUPTED", interrupted.payload["error"]["code"])

        recovered = self.run_cli(*resume).payload

        self.assertEqual("run_completed", recovered["kind"])
        self.assertTrue((self.root / "delivery/.datajig-commit.json").is_file())

    def test_execution_rejects_source_drift_after_preparation(self) -> None:
        source = self.root / "rows.csv"
        source.write_text("id,value\na,1\n", encoding="utf-8")
        planned = self.run_cli(
            "run",
            "from rows.csv source-id-field id export id-field id",
            "--strict",
            "--output",
            "delivery",
        ).payload
        from datajig import run as run_module

        real_prepare = run_module._prepare_run_source

        def prepare_then_mutate(*args: object, **kwargs: object):
            prepared = real_prepare(*args, **kwargs)  # type: ignore[arg-type]
            source.write_text("id,value\na,changed\n", encoding="utf-8")
            return prepared

        with (
            patch.dict(os.environ, {"DATAJIG_NATIVE": str(self.native)}),
            patch.object(run_module, "_prepare_run_source", side_effect=prepare_then_mutate),
            self.assertRaises(PipelineError) as raised,
        ):
            execute_accepted_run(
                {**planned, "kind": "run_plan_accepted", "decision": "ready"}, self.root
            )

        self.assertEqual("SOURCE_CHANGED", raised.exception.code)
        self.assertFalse((self.root / "delivery").exists())


if __name__ == "__main__":
    import unittest

    unittest.main()
