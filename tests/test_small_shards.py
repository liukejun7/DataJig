from __future__ import annotations

import json
from pathlib import Path

from tests.cli_harness import DataJigCliTestCase


class SmallShardExportTests(DataJigCliTestCase):
    def test_relative_split_weights_are_normalized_deterministically(self) -> None:
        self.initialize_jsonl(
            [{"id": f"row-{index}", "value": index} for index in range(20)]
        )
        output = self.root / "ratio-shards"

        exported = self.run_cli(
            "export",
            "--state",
            self.state,
            "--output",
            output,
            "--split",
            "train=7",
            "--split",
            "val=2",
            "--split",
            "test=1",
        ).payload["artifact"]

        manifest = json.loads(Path(exported["manifest"]).read_text(encoding="utf-8"))
        self.assertEqual(
            [
                {"name": "test", "weight": 1000},
                {"name": "train", "weight": 7000},
                {"name": "val", "weight": 2000},
            ],
            manifest["config"]["splits"],
        )

    def test_one_record_and_one_byte_targets_export_valid_oversize_singletons(self) -> None:
        self.initialize_jsonl(
            [
                {"id": "alpha", "text": "first record exceeds one byte"},
                {"id": "beta", "text": "second record exceeds one byte"},
            ]
        )
        output = self.root / "tiny-shards"

        exported = self.run_cli(
            "export",
            "--state",
            self.state,
            "--output",
            output,
            "--split",
            "train=10000",
            "--max-shard-records",
            "1",
            "--max-shard-bytes",
            "1",
        ).payload["artifact"]

        inspected = self.run_cli(
            "export-info",
            Path(exported["manifest"]),
            "--verify",
            "--consumer-plan",
        ).payload["artifact"]
        shards = inspected["consumer_plan"]["splits"][0]["shards"]
        self.assertEqual(2, len(shards))
        self.assertTrue(all(shard["records"] == 1 for shard in shards))
        self.assertTrue(all(shard["bytes"] > 1 for shard in shards))


if __name__ == "__main__":
    import unittest

    unittest.main()
