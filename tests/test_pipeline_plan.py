from __future__ import annotations

import json
import shutil
from pathlib import Path

from tests.cli_harness import DataJigCliTestCase

CONFIG = """\
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
validate:
  quality_policy:
    inline: {}
export:
  split: [train=7, val=2, test=1]
  max_shard_records: 10000
consumption_plan:
  - consumer: huggingface
    split: train
    run_id: run-2026-10-06
"""


class PipelinePlanTests(DataJigCliTestCase):
    def _project(self, root: Path, config: str = CONFIG) -> Path:
        (root / "data").mkdir(parents=True)
        (root / "data" / "events.csv").write_text(
            "user_id,amount\na,1\nb,2\na,3\n", encoding="utf-8"
        )
        path = root / "pipeline.yaml"
        path.write_text(config, encoding="utf-8")
        return path

    def test_plan_is_relocation_stable_and_paths_are_logical(self) -> None:
        first = self.root / "first"
        second = self.root / "second"
        first_config = self._project(first)
        second_config = self._project(second)

        first_result = self.run_cli(
            "pipeline", "plan", "--config", first_config, "--plan", first / "plan.json"
        )
        second_result = self.run_cli(
            "pipeline", "plan", "--config", second_config, "--plan", second / "plan.json"
        )

        first_plan = first_result.payload["artifact"]
        second_plan = second_result.payload["artifact"]
        self.assertEqual(first_plan["pipeline_id"], second_plan["pipeline_id"])
        self.assertEqual(first_plan["bundle_spec_id"], second_plan["bundle_spec_id"])
        self.assertEqual("prepared/training.jsonl", first_plan["target"]["dataset"])
        self.assertNotIn(str(first.resolve()), json.dumps(first_plan, sort_keys=True))

    def test_authorized_target_and_consumption_change_only_the_right_identity(self) -> None:
        base = self.root / "base"
        target_changed = self.root / "target-changed"
        consumption_changed = self.root / "consumption-changed"
        base_config = self._project(base)
        target_config = self._project(
            target_changed,
            CONFIG.replace("prepared/training.jsonl", "prepared/other.jsonl"),
        )
        consumption_config = self._project(
            consumption_changed,
            CONFIG.replace("run-2026-10-06", "run-2026-10-07"),
        )

        plans = []
        for index, config in enumerate((base_config, target_config, consumption_config)):
            plans.append(
                self.run_cli(
                    "pipeline",
                    "plan",
                    "--config",
                    config,
                    "--plan",
                    config.parent / f"plan-{index}.json",
                ).payload["artifact"]
            )

        self.assertNotEqual(plans[0]["pipeline_id"], plans[1]["pipeline_id"])
        self.assertNotEqual(plans[0]["pipeline_id"], plans[2]["pipeline_id"])
        self.assertEqual(plans[0]["bundle_spec_id"], plans[1]["bundle_spec_id"])
        self.assertEqual(plans[0]["bundle_spec_id"], plans[2]["bundle_spec_id"])

    def test_yaml_rejects_alias_tags_duplicate_keys_and_implicit_timestamps(self) -> None:
        hostile = {
            "alias": CONFIG.replace("name: user-agg-train", "name: &name user-agg-train").replace(
                "provider: duckdb", "provider: *name"
            ),
            "tag": CONFIG.replace("name: user-agg-train", "name: !!python/object:builtins.str {}"),
            "duplicate": CONFIG.replace(
                "  provider: duckdb", "  provider: duckdb\n  provider: duckdb"
            ),
            "timestamp": CONFIG.replace("name: user-agg-train", "name: 2026-10-06"),
        }
        for name, payload in hostile.items():
            with self.subTest(name=name):
                project = self.root / name
                config = self._project(project, payload)
                result = self.assert_cli_error(
                    "INVALID_PIPELINE_CONFIG",
                    "pipeline",
                    "plan",
                    "--config",
                    config,
                    "--plan",
                    project / "plan.json",
                )
                self.assertIn("YAML", result.payload["error"]["message"])
                self.assertFalse((project / "plan.json").exists())

    def test_info_verify_rejects_plan_identity_tampering(self) -> None:
        config = self._project(self.root / "project")
        plan_path = config.parent / "plan.json"
        planned = self.run_cli("pipeline", "plan", "--config", config, "--plan", plan_path).payload[
            "artifact"
        ]
        payload = json.loads(plan_path.read_text(encoding="utf-8"))
        payload["pipeline"]["name"] = "tampered"
        plan_path.write_text(json.dumps(payload), encoding="utf-8")

        result = self.assert_cli_error(
            "PIPELINE_PLAN_TAMPERED", "pipeline", "info", plan_path, "--verify"
        )
        self.assertEqual(planned["pipeline_id"], result.payload["error"]["expected_id"])

    def test_info_verify_rejects_transform_tampering_behind_bundle_id(self) -> None:
        config = self._project(self.root / "project")
        plan_path = config.parent / "plan.json"
        planned = self.run_cli("pipeline", "plan", "--config", config, "--plan", plan_path).payload[
            "artifact"
        ]
        payload = json.loads(plan_path.read_text(encoding="utf-8"))
        payload["transform"]["sql"] = "SELECT 'forged' AS id ORDER BY id"
        plan_path.write_text(json.dumps(payload), encoding="utf-8")

        result = self.assert_cli_error(
            "PIPELINE_PLAN_TAMPERED", "pipeline", "info", plan_path, "--verify"
        )
        self.assertEqual(planned["bundle_spec_id"], result.payload["error"]["expected_id"])

    def test_plan_refuses_unknown_fields_and_existing_destination(self) -> None:
        config = self._project(
            self.root / "project",
            CONFIG.replace("schema_version: 1", "schema_version: 1\nsecret: no"),
        )
        plan_path = config.parent / "plan.json"
        plan_path.write_text("existing", encoding="utf-8")
        result = self.assert_cli_error(
            "INVALID_PIPELINE_CONFIG",
            "pipeline",
            "plan",
            "--config",
            config,
            "--plan",
            plan_path,
        )
        self.assertIn("unknown", result.payload["error"]["message"].lower())
        self.assertEqual("existing", plan_path.read_text(encoding="utf-8"))

    def test_input_must_be_single_regular_file_and_within_limits(self) -> None:
        project = self.root / "project"
        config = self._project(project)
        shutil.rmtree(project / "data")
        (project / "data").mkdir()
        result = self.assert_cli_error(
            "INVALID_PIPELINE_CONFIG",
            "pipeline",
            "plan",
            "--config",
            config,
            "--plan",
            project / "plan.json",
        )
        self.assertIn("regular file", result.payload["error"]["message"])

    def test_transform_params_reject_floating_point_values(self) -> None:
        project = self.root / "project"
        config = self._project(project, CONFIG.replace("  params: []", "  params: [1.5]"))

        result = self.assert_cli_error(
            "INVALID_PIPELINE_CONFIG",
            "pipeline",
            "plan",
            "--config",
            config,
            "--plan",
            project / "plan.json",
        )

        self.assertIn("string, boolean, integer, or null", result.payload["error"]["message"])
        self.assertFalse((project / "plan.json").exists())

    def test_target_and_delivery_paths_reject_symlinked_parents(self) -> None:
        project = self.root / "project"
        config = self._project(project)
        external = self.root / "external"
        external.mkdir()
        (project / "prepared").symlink_to(external, target_is_directory=True)

        result = self.assert_cli_error(
            "INVALID_PIPELINE_CONFIG",
            "pipeline",
            "plan",
            "--config",
            config,
            "--plan",
            project / "plan.json",
        )
        self.assertIn("symbolic link", result.payload["error"]["message"])
