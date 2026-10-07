from __future__ import annotations

import json
import os

from datajig.pipeline import PipelineError
from datajig.run import _run_receipt_identity, _verify_run_provider_binding
from tests.cli_harness import DataJigCliTestCase


class RunSecurityRegressionTests(DataJigCliTestCase):
    def test_provider_drift_is_rejected_before_pipeline_apply(self) -> None:
        expected = {"provider_id": "provider_" + "a" * 64, "version": "one"}
        actual = {"provider_id": "provider_" + "b" * 64, "version": "two"}

        with self.assertRaises(PipelineError) as raised:
            _verify_run_provider_binding(expected, {"provider_identity": actual})

        self.assertEqual("RUN_PROVIDER_DRIFT", raised.exception.code)
        self.assertEqual(expected["provider_id"], raised.exception.details["expected_provider_id"])

    def test_status_fails_closed_when_a_run_receipt_is_tampered(self) -> None:
        source = self.root / "rows.csv"
        source.write_text("id,value\na,1\n", encoding="utf-8")
        completed = self.run_cli(
            "run",
            "from rows.csv source-id-field id export id-field id",
            "--output",
            "delivery",
        ).payload["artifact"]
        receipt_path = self.root / ".datajig" / "runs" / completed["intent_id"] / "receipts" / (
            completed["attempt_id"] + ".json"
        )
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt["bundle_id"] = "bundle_" + "0" * 64
        receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
        state = (
            self.root
            / ".datajig"
            / "runs"
            / completed["intent_id"]
            / "workspace"
            / "state"
        )

        failed = self.run_cli("status", "--state", state, expected_returncode=2)

        self.assertEqual("RUN_RECEIPT_TAMPERED", failed.payload["error"]["code"])

    def test_lineage_resolves_receipts_older_than_the_bounded_status_index(self) -> None:
        source = self.root / "rows.csv"
        source.write_text("id,value\na,1\n", encoding="utf-8")
        completed = self.run_cli(
            "run",
            "from rows.csv source-id-field id export id-field id",
            "--output",
            "delivery",
        ).payload["artifact"]
        old_receipt_path = self.root / completed["run_receipt_path"]
        old_receipt = json.loads(old_receipt_path.read_text(encoding="utf-8"))
        lookup_path = (
            self.root
            / ".datajig"
            / "meta"
            / "run-receipts"
            / f"{completed['run_receipt_id']}.json"
        )
        lookup_path.unlink()
        os.utime(old_receipt_path, ns=(1, 1))
        receipts = old_receipt_path.parent
        for index in range(64):
            synthetic = dict(old_receipt)
            synthetic["attempt_id"] = f"attempt_{index + 1:064x}"
            basis = {
                key: value for key, value in synthetic.items() if key != "run_receipt_id"
            }
            synthetic["run_receipt_id"] = _run_receipt_identity(basis)
            path = receipts / f"synthetic-{index:02d}.json"
            path.write_text(json.dumps(synthetic), encoding="utf-8")
            os.utime(path, ns=(index + 2, index + 2))
        state = (
            self.root
            / ".datajig"
            / "runs"
            / completed["intent_id"]
            / "workspace"
            / "state"
        )
        self.run_cli("status", "--state", state)

        resolved = self.run_cli(
            "lineage", completed["run_receipt_id"], "--state", state
        ).payload["artifact"]

        self.assertEqual(completed["run_receipt_id"], resolved["run_receipt_id"])

    def test_lineage_id_lookup_ignores_an_oversized_unrelated_receipt(self) -> None:
        source = self.root / "rows.csv"
        source.write_text("id,value\na,1\n", encoding="utf-8")
        completed = self.run_cli(
            "run",
            "from rows.csv source-id-field id export id-field id",
            "--output",
            "delivery",
        ).payload["artifact"]
        receipt_path = self.root / completed["run_receipt_path"]
        (receipt_path.parent / "unrelated.json").write_bytes(b"{" + b"x" * (9 * 1024 * 1024))
        state = (
            self.root
            / ".datajig"
            / "runs"
            / completed["intent_id"]
            / "workspace"
            / "state"
        )

        resolved = self.run_cli(
            "lineage", completed["run_receipt_id"], "--state", state
        ).payload["artifact"]

        self.assertEqual(completed["run_receipt_id"], resolved["run_receipt_id"])


if __name__ == "__main__":
    import unittest

    unittest.main()
