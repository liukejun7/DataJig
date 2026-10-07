from __future__ import annotations

import json

from datajig.pipeline import PipelineError
from datajig.run import _verify_run_provider_binding
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


if __name__ == "__main__":
    import unittest

    unittest.main()
