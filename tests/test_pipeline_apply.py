from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path

from datajig.pipeline import _exchange_directories, _rename_directory_noreplace
from tests.cli_harness import DataJigCliTestCase


class PipelineApplyTests(DataJigCliTestCase):
    def test_atomic_directory_exchange_swaps_both_directories_and_can_reverse(self) -> None:
        left = self.root / "left"
        right = self.root / "right"
        left.mkdir()
        right.mkdir()
        (left / "left.txt").write_text("left", encoding="utf-8")
        (right / "right.txt").write_text("right", encoding="utf-8")

        _exchange_directories(left, right)
        self.assertEqual("right", (left / "right.txt").read_text(encoding="utf-8"))
        self.assertEqual("left", (right / "left.txt").read_text(encoding="utf-8"))

        _exchange_directories(left, right)
        self.assertEqual("left", (left / "left.txt").read_text(encoding="utf-8"))
        self.assertEqual("right", (right / "right.txt").read_text(encoding="utf-8"))

    def test_atomic_directory_publish_does_not_replace_existing_target(self) -> None:
        source = self.root / "staged"
        target = self.root / "published"
        source.mkdir()
        target.mkdir()
        (target / "sentinel").write_text("owned", encoding="utf-8")

        with self.assertRaises(FileExistsError):
            _rename_directory_noreplace(source, target)

        self.assertTrue(source.is_dir())
        self.assertEqual("owned", (target / "sentinel").read_text(encoding="utf-8"))

    def _project(self, name: str = "project") -> tuple[Path, Path]:
        project = self.root / name
        (project / "data").mkdir(parents=True)
        rows = ["user_id,amount"] + [f"user-{index % 7},{index}" for index in range(120)]
        source = project / "data" / "events.csv"
        source.write_text("\n".join(rows) + "\n", encoding="utf-8")
        config = project / "pipeline.yaml"
        config.write_text(
            """\
schema_version: 1
pipeline:
  name: user-agg-train
  provider: duckdb
target:
  dataset: prepared/training.jsonl
  state: workspace/.datajig
  mode: create
delivery:
  output: deliveries/user-agg-train
inputs:
  - alias: events
    path: data/events.csv
    format: csv
transform:
  sql: |
    SELECT user_id AS id, CAST(SUM(CAST(amount AS INTEGER)) AS VARCHAR) AS total
    FROM events GROUP BY user_id ORDER BY id
  id_field: id
  params: []
export:
  split: [train=7, val=2, test=1]
  max_shard_records: 3
  max_shard_bytes: 1024
consumption_plan:
  - consumer: huggingface
    split: train
    run_id: run-2026-10-06
""",
            encoding="utf-8",
        )
        return config, source

    def _plan(self, config: Path) -> tuple[Path, dict[str, object]]:
        plan = config.parent / f"{config.stem}-pipeline-plan.json"
        artifact = self.run_cli("pipeline", "plan", "--config", config, "--plan", plan).payload[
            "artifact"
        ]
        return plan, artifact

    def test_create_apply_publishes_bound_delivery_and_is_idempotent(self) -> None:
        config, _ = self._project()
        plan, planned = self._plan(config)

        applied = self.run_cli(
            "pipeline", "apply", plan, "--accept-plan", planned["pipeline_id"]
        ).payload

        receipt = applied["artifact"]
        delivery = config.parent / "deliveries" / "user-agg-train"
        marker = json.loads((delivery / ".datajig-commit.json").read_text(encoding="utf-8"))
        self.assertEqual("committed", receipt["status"])
        self.assertEqual(planned["pipeline_id"], receipt["pipeline_id"])
        self.assertEqual(receipt["pipeline_receipt_id"], marker["pipeline_receipt_id"])
        self.assertTrue((delivery / "bundle" / "datajig.bundle.json").is_file())
        self.assertEqual(1, len(receipt["consumption_plans"]))
        self.assertTrue((config.parent / "prepared" / "training.jsonl").is_file())

        repeated = self.run_cli(
            "pipeline", "apply", plan, "--accept-plan", planned["pipeline_id"]
        ).payload
        self.assertEqual("already_applied", repeated["decision"])
        self.assertEqual(
            receipt["pipeline_receipt_id"], repeated["artifact"]["pipeline_receipt_id"]
        )

    def test_apply_rejects_acceptance_or_input_drift_before_publication(self) -> None:
        config, source = self._project()
        plan, planned = self._plan(config)
        wrong = self.assert_cli_error(
            "PIPELINE_ACCEPTANCE_MISMATCH",
            "pipeline",
            "apply",
            plan,
            "--accept-plan",
            "pipe_" + "0" * 64,
        )
        self.assertEqual(planned["pipeline_id"], wrong.payload["error"]["expected_id"])
        self.assertEqual(
            [{"command": "pipeline", "args": ["--help"]}],
            wrong.payload["next_actions"],
        )

        source.write_text("user_id,amount\na,999\n", encoding="utf-8")
        drift = self.assert_cli_error(
            "PIPELINE_INPUT_DRIFT",
            "pipeline",
            "apply",
            plan,
            "--accept-plan",
            planned["pipeline_id"],
        )
        self.assertEqual("events", drift.payload["error"]["input_alias"])
        self.assertFalse((config.parent / "deliveries").exists())
        self.assertFalse((config.parent / "prepared" / "training.jsonl").exists())

    def test_resume_finishes_delivery_after_head_commit_failpoint(self) -> None:
        config, _ = self._project()
        plan, planned = self._plan(config)
        interrupted = self.run_cli(
            "pipeline",
            "apply",
            plan,
            "--accept-plan",
            planned["pipeline_id"],
            expected_returncode=2,
            extra_env={"DATAJIG_PIPELINE_FAILPOINT": "after_head_commit"},
        )
        self.assertEqual("PIPELINE_INTERRUPTED", interrupted.payload["error"]["code"])
        self.assertEqual("head_committed", interrupted.payload["error"]["phase"])
        self.assertFalse((config.parent / "deliveries" / "user-agg-train").exists())
        self.assertTrue((config.parent / "workspace" / ".datajig" / "refs.json").is_file())

        blocked = self.assert_cli_error(
            "PIPELINE_RECOVERY_REQUIRED",
            "pipeline",
            "apply",
            plan,
            "--accept-plan",
            planned["pipeline_id"],
        )
        self.assertIn("--resume", blocked.payload["error"]["remediation"])

        resumed = self.run_cli(
            "pipeline",
            "apply",
            plan,
            "--accept-plan",
            planned["pipeline_id"],
            "--resume",
        ).payload["artifact"]
        self.assertEqual("committed", resumed["status"])
        self.assertTrue(
            (config.parent / "deliveries" / "user-agg-train" / ".datajig-commit.json").is_file()
        )

    def test_auto_accept_persists_plan_and_delivers(self) -> None:
        config, _ = self._project()
        applied = self.run_cli("pipeline", "apply", "--config", config, "--auto-accept").payload[
            "artifact"
        ]
        self.assertEqual("committed", applied["status"])
        self.assertTrue((config.parent / ".datajig-pipeline-plan.json").is_file())

    def test_resume_recovers_delivery_renamed_before_marker(self) -> None:
        config, _ = self._project()
        plan, planned = self._plan(config)
        interrupted = self.run_cli(
            "pipeline",
            "apply",
            plan,
            "--accept-plan",
            planned["pipeline_id"],
            expected_returncode=2,
            extra_env={"DATAJIG_PIPELINE_FAILPOINT": "after_delivery_rename"},
        )
        self.assertEqual("PIPELINE_INTERRUPTED", interrupted.payload["error"]["code"])
        delivery = config.parent / "deliveries" / "user-agg-train"
        self.assertTrue(delivery.is_dir())
        self.assertFalse((delivery / ".datajig-commit.json").exists())

        resumed = self.run_cli(
            "pipeline",
            "apply",
            plan,
            "--accept-plan",
            planned["pipeline_id"],
            "--resume",
        ).payload["artifact"]
        self.assertEqual("committed", resumed["status"])
        self.assertTrue((delivery / ".datajig-commit.json").is_file())

    def test_rerun_recovers_marker_published_before_journal_update(self) -> None:
        config, _ = self._project()
        plan, planned = self._plan(config)
        interrupted = self.run_cli(
            "pipeline",
            "apply",
            plan,
            "--accept-plan",
            planned["pipeline_id"],
            expected_returncode=2,
            extra_env={"DATAJIG_PIPELINE_FAILPOINT": "after_delivery_marker"},
        )
        self.assertEqual("delivery_marker_published", interrupted.payload["error"]["phase"])

        recovered = self.run_cli(
            "pipeline", "apply", plan, "--accept-plan", planned["pipeline_id"]
        ).payload
        self.assertEqual("already_applied", recovered["decision"])
        stored = (
            config.parent
            / "workspace"
            / ".datajig"
            / "pipelines"
            / planned["pipeline_id"]
            / "receipt.json"
        )
        self.assertTrue(stored.is_file())

    def test_existing_unowned_delivery_is_never_replaced(self) -> None:
        config, _ = self._project()
        delivery = config.parent / "deliveries" / "user-agg-train"
        delivery.mkdir(parents=True)
        sentinel = delivery / "owner.txt"
        sentinel.write_text("keep", encoding="utf-8")
        result = self.assert_cli_error(
            "INVALID_PIPELINE_CONFIG",
            "pipeline",
            "plan",
            "--config",
            config,
            "--plan",
            config.parent / "pipeline-plan.json",
        )
        self.assertIn("delivery.output", result.payload["error"]["message"])
        self.assertEqual("keep", sentinel.read_text(encoding="utf-8"))

    def test_plan_lock_rejects_concurrent_apply(self) -> None:
        config, _ = self._project()
        plan, planned = self._plan(config)
        lock = (
            config.parent
            / "workspace"
            / ".datajig-pipeline"
            / planned["pipeline_id"]
            / "pipeline.lock"
        )
        lock.parent.mkdir(parents=True)
        descriptor = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = self.assert_cli_error(
                "PIPELINE_BUSY",
                "pipeline",
                "apply",
                plan,
                "--accept-plan",
                planned["pipeline_id"],
            )
            self.assertIn("OS lock", result.payload["error"]["message"])
        finally:
            os.close(descriptor)

    def test_update_advances_head_and_no_op_still_delivers(self) -> None:
        config, source = self._project()
        first_plan, first_planned = self._plan(config)
        first = self.run_cli(
            "pipeline", "apply", first_plan, "--accept-plan", first_planned["pipeline_id"]
        ).payload["artifact"]

        source.write_text(source.read_text(encoding="utf-8") + "user-new,500\n", encoding="utf-8")
        update_text = config.read_text(encoding="utf-8").replace("mode: create", "mode: update")
        update_text = update_text.replace("name: user-agg-train", "name: user-agg-train-v2")
        update_text = update_text.replace(
            "deliveries/user-agg-train", "deliveries/user-agg-train-v2"
        )
        update_text = update_text.replace("run-2026-10-06", "run-2026-10-07")
        update_config = config.parent / "update.yaml"
        update_config.write_text(update_text, encoding="utf-8")
        update_plan, update_planned = self._plan(update_config)

        updated = self.run_cli(
            "pipeline",
            "apply",
            update_plan,
            "--accept-plan",
            update_planned["pipeline_id"],
        ).payload["artifact"]
        self.assertEqual(first["final_revision"], updated["base_revision"])
        self.assertNotEqual(updated["base_revision"], updated["final_revision"])
        self.assertFalse(updated["no_op"])
        revision = self.run_cli(
            "log", "--state", config.parent / "workspace" / ".datajig", "--limit", 1
        ).payload["artifact"]["revisions"][0]
        self.assertEqual(updated["transform_plan_id"], revision["transform_lineage"]["plan_id"])

        no_op_text = update_text.replace("user-agg-train-v2", "user-agg-train-v3").replace(
            "run-2026-10-07", "run-2026-10-08"
        )
        no_op_config = config.parent / "no-op.yaml"
        no_op_config.write_text(no_op_text, encoding="utf-8")
        no_op_plan, no_op_planned = self._plan(no_op_config)
        no_op = self.run_cli(
            "pipeline", "apply", no_op_plan, "--accept-plan", no_op_planned["pipeline_id"]
        ).payload["artifact"]
        self.assertTrue(no_op["no_op"])
        self.assertEqual(updated["final_revision"], no_op["final_revision"])
        self.assertTrue(
            (config.parent / "deliveries" / "user-agg-train-v3" / ".datajig-commit.json").is_file()
        )

    def test_update_atomically_replaces_owned_fixed_delivery_and_retains_backup(self) -> None:
        config, source = self._project()
        first_plan, first_planned = self._plan(config)
        first = self.run_cli(
            "pipeline", "apply", first_plan, "--accept-plan", first_planned["pipeline_id"]
        ).payload["artifact"]
        delivery = config.parent / "deliveries" / "user-agg-train"

        source.write_text(source.read_text(encoding="utf-8") + "user-new,500\n", encoding="utf-8")
        update_text = config.read_text(encoding="utf-8").replace("mode: create", "mode: update")
        update_text = update_text.replace("name: user-agg-train", "name: user-agg-train-v2", 1)
        update_text = update_text.replace("run-2026-10-06", "run-2026-10-07")
        update_config = config.parent / "fixed-update.yaml"
        update_config.write_text(update_text, encoding="utf-8")
        update_plan, update_planned = self._plan(update_config)

        updated = self.run_cli(
            "pipeline", "apply", update_plan, "--accept-plan", update_planned["pipeline_id"]
        ).payload["artifact"]

        marker = json.loads((delivery / ".datajig-commit.json").read_text(encoding="utf-8"))
        backup = Path(str(updated["delivery_backup"]))
        old_marker = json.loads((backup / ".datajig-commit.json").read_text(encoding="utf-8"))
        self.assertEqual(update_planned["pipeline_id"], marker["pipeline_id"])
        self.assertEqual(first["pipeline_receipt_id"], old_marker["pipeline_receipt_id"])
        self.assertEqual(first["final_revision"], updated["base_revision"])
        self.assertTrue(backup.is_dir())

    def test_delivery_exchange_crash_recovers_forward_to_new_marker(self) -> None:
        config, source = self._project()
        first_plan, first_planned = self._plan(config)
        first = self.run_cli(
            "pipeline", "apply", first_plan, "--accept-plan", first_planned["pipeline_id"]
        ).payload["artifact"]
        delivery = config.parent / "deliveries" / "user-agg-train"

        source.write_text(source.read_text(encoding="utf-8") + "user-new,500\n", encoding="utf-8")
        update_text = config.read_text(encoding="utf-8").replace("mode: create", "mode: update")
        update_text = update_text.replace("name: user-agg-train", "name: user-agg-train-v2", 1)
        update_text = update_text.replace("run-2026-10-06", "run-2026-10-07")
        update_config = config.parent / "exchange-recovery.yaml"
        update_config.write_text(update_text, encoding="utf-8")
        update_plan, update_planned = self._plan(update_config)

        interrupted = self.run_cli(
            "pipeline",
            "apply",
            update_plan,
            "--accept-plan",
            update_planned["pipeline_id"],
            expected_returncode=2,
            extra_env={"DATAJIG_PIPELINE_FAILPOINT": "after_delivery_exchange"},
        ).payload
        self.assertEqual("delivery_exchanged", interrupted["error"]["phase"])
        self.assertFalse((delivery / ".datajig-commit.json").exists())
        self.assertTrue((delivery / "pipeline-receipt.json").is_file())

        resumed = self.run_cli(
            "pipeline",
            "apply",
            update_plan,
            "--accept-plan",
            update_planned["pipeline_id"],
            "--resume",
        ).payload["artifact"]

        marker = json.loads((delivery / ".datajig-commit.json").read_text(encoding="utf-8"))
        self.assertEqual(update_planned["pipeline_id"], marker["pipeline_id"])
        self.assertEqual(resumed["pipeline_receipt_id"], marker["pipeline_receipt_id"])
        self.assertNotEqual(first["final_revision"], resumed["final_revision"])

    def test_fixed_delivery_update_fails_closed_on_receipt_marker_mismatch(self) -> None:
        config, source = self._project()
        first_plan, first_planned = self._plan(config)
        self.run_cli(
            "pipeline", "apply", first_plan, "--accept-plan", first_planned["pipeline_id"]
        )
        delivery = config.parent / "deliveries" / "user-agg-train"
        receipt_path = delivery / "pipeline-receipt.json"
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt["bundle_id"] = "bundle_" + "0" * 64
        receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

        source.write_text(source.read_text(encoding="utf-8") + "user-new,500\n", encoding="utf-8")
        update_text = config.read_text(encoding="utf-8").replace("mode: create", "mode: update")
        update_text = update_text.replace("name: user-agg-train", "name: user-agg-train-v2", 1)
        update_config = config.parent / "tampered-update.yaml"
        update_config.write_text(update_text, encoding="utf-8")

        failed = self.assert_cli_error(
            "DELIVERY_CONFLICT",
            "pipeline",
            "plan",
            "--config",
            update_config,
            "--plan",
            config.parent / "tampered-update-plan.json",
        )
        self.assertIn("marker and receipt differ", failed.payload["error"]["message"])

    def test_update_resumes_after_dataset_replacement(self) -> None:
        config, source = self._project()
        first_plan, first_planned = self._plan(config)
        self.run_cli("pipeline", "apply", first_plan, "--accept-plan", first_planned["pipeline_id"])
        source.write_text(source.read_text(encoding="utf-8") + "user-new,500\n", encoding="utf-8")
        update_text = config.read_text(encoding="utf-8").replace("mode: create", "mode: update")
        update_text = update_text.replace("name: user-agg-train", "name: recover-update")
        update_text = update_text.replace("deliveries/user-agg-train", "deliveries/recover-update")
        update_config = config.parent / "recover-update.yaml"
        update_config.write_text(update_text, encoding="utf-8")
        plan, planned = self._plan(update_config)

        interrupted = self.run_cli(
            "pipeline",
            "apply",
            plan,
            "--accept-plan",
            planned["pipeline_id"],
            expected_returncode=2,
            extra_env={"DATAJIG_PIPELINE_FAILPOINT": "after_dataset_replace"},
        )
        self.assertEqual("dataset_replaced", interrupted.payload["error"]["phase"])
        resumed = self.run_cli(
            "pipeline",
            "apply",
            plan,
            "--accept-plan",
            planned["pipeline_id"],
            "--resume",
        ).payload["artifact"]
        self.assertFalse(resumed["no_op"])
        self.assertNotEqual(resumed["base_revision"], resumed["final_revision"])

    def test_update_prepares_detached_revision_before_head_cas(self) -> None:
        config, source = self._project()
        first_plan, first_planned = self._plan(config)
        first = self.run_cli(
            "pipeline", "apply", first_plan, "--accept-plan", first_planned["pipeline_id"]
        ).payload["artifact"]
        source.write_text(source.read_text(encoding="utf-8") + "user-new,500\n", encoding="utf-8")
        update_text = config.read_text(encoding="utf-8").replace("mode: create", "mode: update")
        update_text = update_text.replace("name: user-agg-train", "name: detached-update")
        update_text = update_text.replace("deliveries/user-agg-train", "deliveries/detached-update")
        update_config = config.parent / "detached-update.yaml"
        update_config.write_text(update_text, encoding="utf-8")
        plan, planned = self._plan(update_config)

        interrupted = self.run_cli(
            "pipeline",
            "apply",
            plan,
            "--accept-plan",
            planned["pipeline_id"],
            expected_returncode=2,
            extra_env={"DATAJIG_PIPELINE_FAILPOINT": "after_detached_prepare"},
        )
        self.assertEqual("revision_prepared", interrupted.payload["error"]["phase"])
        current = self.run_cli("log", "--state", config.parent / "workspace" / ".datajig")
        self.assertEqual(
            first["final_revision"], current.payload["artifact"]["revisions"][0]["revision_id"]
        )
        self.assertFalse((config.parent / "deliveries" / "detached-update").exists())

        resumed = self.run_cli(
            "pipeline",
            "apply",
            plan,
            "--accept-plan",
            planned["pipeline_id"],
            "--resume",
        ).payload["artifact"]
        self.assertEqual(first["final_revision"], resumed["base_revision"])
        self.assertNotEqual(resumed["base_revision"], resumed["final_revision"])
