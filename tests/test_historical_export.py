from __future__ import annotations

import json
from pathlib import Path
from typing import Any, ClassVar

from tests.cli_harness import DataJigCliTestCase


class HistoricalExportTests(DataJigCliTestCase):
    ancestor_records: ClassVar[list[dict[str, str]]] = [
        {"id": "alpha", "split": "old"},
        {"id": "beta", "split": "old"},
    ]
    head_records: ClassVar[list[dict[str, str]]] = [
        {"id": "alpha", "split": "current"},
        {"id": "gamma", "split": "current"},
    ]

    def _initialize_history(self) -> tuple[Path, dict[str, Any], dict[str, Any]]:
        dataset, initialized = self.initialize_jsonl(self.ancestor_records)
        ancestor_revision_id = initialized["head_revision_id"]
        history = self.run_cli("log", "--state", self.state).payload["artifact"]
        ancestor = history["revisions"][0]
        self.assertEqual(ancestor_revision_id, ancestor["revision_id"])

        change_id, _ = self.begin_change("advance-historical-head")
        self.write_jsonl(dataset, self.head_records)
        changeset_id, _ = self.stage_change(change_id)
        review = self.check_change(change_id, changeset_id)
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
            "Advance beyond historical export target",
        ).payload["artifact"]
        self.assertNotEqual(ancestor_revision_id, sealed["revision_id"])

        history = self.run_cli("log", "--state", self.state).payload["artifact"]
        self.assertEqual(2, history["returned"])
        current, reachable_ancestor = history["revisions"]
        self.assertEqual(sealed["revision_id"], current["revision_id"])
        self.assertEqual(ancestor_revision_id, reachable_ancestor["revision_id"])
        self.assertEqual(ancestor_revision_id, current["parent"])
        self.assertNotEqual(current["state_id"], reachable_ancestor["state_id"])
        self.assertNotEqual(
            current["dataset_content_id"], reachable_ancestor["dataset_content_id"]
        )
        self.assertEqual(self.head_records, self._read_records(dataset))
        return dataset, reachable_ancestor, current

    def _assert_verified_export(
        self,
        ancestor: dict[str, Any],
        current: dict[str, Any],
        output: Path,
    ) -> None:
        exported = self.run_cli(
            "export",
            "--state",
            self.state,
            "--revision",
            ancestor["revision_id"],
            "--output",
            output,
            "--split",
            "train=10000",
        ).payload["artifact"]
        self.assertEqual(ancestor["revision_id"], exported["source_revision_id"])
        self.assertEqual(ancestor["state_id"], exported["source_state_id"])
        self.assertNotEqual(current["revision_id"], exported["source_revision_id"])
        self.assertNotEqual(current["state_id"], exported["source_state_id"])
        manifest = Path(exported["manifest"])
        manifest_payload = json.loads(manifest.read_text())
        self.assertEqual(ancestor["revision_id"], manifest_payload["source"]["revision_id"])
        self.assertEqual(ancestor["state_id"], manifest_payload["source"]["state_id"])
        verified = self.run_cli(
            "export-info",
            manifest,
            "--verify",
            "--expect-revision",
            ancestor["revision_id"],
        ).payload["artifact"]
        self.assertTrue(verified["verified"])
        self.assertEqual(ancestor["revision_id"], verified["source_revision_id"])
        self.assertEqual(ancestor["state_id"], verified["source_state_id"])
        self.assertNotEqual(current["revision_id"], verified["source_revision_id"])
        shard_paths = sorted(output.rglob("*.jsonl"))
        self.assertEqual(1, len(shard_paths))
        shard_records = [json.loads(line) for line in shard_paths[0].read_text().splitlines()]
        self.assertCountEqual(self.ancestor_records, shard_records)
        self.assertNotEqual(self.head_records, shard_records)

    def _blob_path(self, dataset_content_id: str) -> Path:
        blob = self.state / "objects" / "jsonl-blobs" / f"{dataset_content_id}.jsonl"
        self.assertTrue(blob.is_file())
        return blob

    def test_verified_historical_export_survives_deleted_live_dataset(self) -> None:
        dataset, ancestor, current = self._initialize_history()
        dataset.unlink()

        self._assert_verified_export(ancestor, current, self.root / "deleted-live-export")
        self.assert_no_staging_artifacts()

    def test_verified_historical_export_ignores_diverged_live_dataset(self) -> None:
        dataset, ancestor, current = self._initialize_history()
        self.write_jsonl(dataset, [{"id": "replacement", "split": "live"}])

        self._assert_verified_export(ancestor, current, self.root / "diverged-live-export")
        self.assert_no_staging_artifacts()

    def test_missing_historical_blob_has_stable_error_and_publishes_nothing(self) -> None:
        dataset, ancestor, current = self._initialize_history()
        current_bytes = dataset.read_bytes()
        self._blob_path(ancestor["dataset_content_id"]).unlink()
        output = self.root / "missing-blob-export"

        result = self.run_cli(
            "export",
            "--state",
            self.state,
            "--revision",
            ancestor["revision_id"],
            "--output",
            output,
            "--split",
            "train=10000",
            expected_returncode=4,
        )
        self.assertEqual("REVISION_CONTENT_UNAVAILABLE", result.payload["error"]["code"])
        self.assertFalse(output.exists())
        self.assertEqual(current_bytes, dataset.read_bytes())
        self.assertEqual(current["revision_id"], self.status()["head_revision_id"])
        self.assert_no_staging_artifacts()

    def test_tampered_historical_blob_has_stable_error_and_publishes_nothing(self) -> None:
        dataset, ancestor, current = self._initialize_history()
        current_bytes = dataset.read_bytes()
        self._blob_path(ancestor["dataset_content_id"]).write_bytes(b'{"id":"tampered"}\n')
        output = self.root / "tampered-blob-export"

        self.assert_cli_error(
            "REVISION_CONTENT_CORRUPT",
            "export",
            "--state",
            self.state,
            "--revision",
            ancestor["revision_id"],
            "--output",
            output,
            "--split",
            "train=10000",
        )
        self.assertFalse(output.exists())
        self.assertEqual(current_bytes, dataset.read_bytes())
        self.assertEqual(current["revision_id"], self.status()["head_revision_id"])
        self.assert_no_staging_artifacts()

    def _read_records(self, dataset: Path) -> list[dict[str, Any]]:
        return [json.loads(line) for line in dataset.read_text().splitlines()]


if __name__ == "__main__":
    import unittest

    unittest.main()
