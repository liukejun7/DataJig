from __future__ import annotations

import json

from tests.cli_harness import DataJigCliTestCase


class PrepareDirectoryTests(DataJigCliTestCase):
    def test_recipe_identity_errors_name_the_invalid_field_and_schema_action(self) -> None:
        source = self.root / "source.csv"
        source.write_text("id,value\na,1\n", encoding="utf-8")
        baseline = {
            "namespace": "datajig",
            "kind": "prepare",
            "schema_version": 1,
            "source": {"format": "csv"},
            "output": {"format": "jsonl"},
            "id_field": "id",
            "steps": [],
        }
        cases = (
            ("namespace", "other", "namespace must be 'datajig'"),
            ("kind", "other", "kind must be 'prepare'"),
            ("schema_version", 99, "schema_version 99 is unsupported; expected 1"),
            ("output", {"format": "csv"}, "output.format must be 'jsonl'"),
        )

        for field, value, expected in cases:
            with self.subTest(field=field):
                recipe = self.root / f"{field}.json"
                recipe.write_text(
                    json.dumps({**baseline, field: value}), encoding="utf-8"
                )
                failed = self.assert_cli_error(
                    "INVALID_ARGUMENT",
                    "prepare-plan",
                    source,
                    "--recipe",
                    recipe,
                    "--output",
                    self.root / f"{field}.jsonl",
                    "--plan",
                    self.root / f"{field}.plan.json",
                ).payload
                self.assertIn(expected, failed["error"]["message"])
                self.assertEqual(
                    [{"command": "artifact-schema", "args": ["prepare-recipe"]}],
                    failed["next_actions"],
                )

    def test_recursive_local_shards_plan_apply_and_detect_membership_drift(self) -> None:
        source = self.root / "raw"
        (source / "year=2025" / "month=02").mkdir(parents=True)
        (source / "year=2025" / "month=01").mkdir(parents=True)
        (source / "year=2025" / "month=02" / "z.csv").write_text(
            "id,value\nz,2\n", encoding="utf-8"
        )
        (source / "year=2025" / "month=01" / "a.csv").write_text(
            "id,value\na,1\n", encoding="utf-8"
        )
        (source / "year=2025" / "month=01" / "ignored.csv").write_text(
            "id,value\nignored,0\n", encoding="utf-8"
        )
        recipe = self.root / "recipe.json"
        recipe.write_text(
            json.dumps(
                {
                    "namespace": "datajig",
                    "kind": "prepare",
                    "schema_version": 1,
                    "source": {
                        "format": "csv",
                        "include": ["**/*.csv"],
                        "ignore": ["**/ignored.csv"],
                    },
                    "output": {"format": "jsonl"},
                    "id_field": "id",
                    "steps": [{"op": "cast", "field": "value", "type": "integer"}],
                }
            ),
            encoding="utf-8",
        )
        output = self.root / "prepared.jsonl"
        plan = self.root / "prepare-plan.json"

        planned = self.run_cli(
            "prepare-plan",
            source,
            "--recipe",
            recipe,
            "--output",
            output,
            "--plan",
            plan,
        ).payload["artifact"]

        self.assertTrue(planned["source_content_id"].startswith("source_set_"))
        self.assertEqual(2, planned["source_rows"])
        self.run_cli("prepare-apply", plan, "--accept-plan", planned["plan_id"])
        self.assertEqual(
            [{"id": "a", "value": 1}, {"id": "z", "value": 2}],
            [json.loads(line) for line in output.read_text().splitlines()],
        )

        second_output = self.root / "second.jsonl"
        second_plan = self.root / "second-plan.json"
        second = self.run_cli(
            "prepare-plan",
            source,
            "--recipe",
            recipe,
            "--output",
            second_output,
            "--plan",
            second_plan,
        ).payload["artifact"]
        (source / "year=2025" / "month=01" / "new.csv").write_text(
            "id,value\nnew,3\n", encoding="utf-8"
        )
        failed = self.assert_cli_error(
            "STALE_PREPARE_INPUT",
            "prepare-apply",
            second_plan,
            "--accept-plan",
            second["plan_id"],
        )
        self.assertIn("changed after planning", failed.payload["error"]["message"])
        self.assertFalse(second_output.exists())


if __name__ == "__main__":
    import unittest

    unittest.main()
