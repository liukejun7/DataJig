from __future__ import annotations

import json

from tests.cli_harness import DataJigCliTestCase


class TrustBoundaryTests(DataJigCliTestCase):
    def test_init_rejects_a_symlinked_quality_policy(self) -> None:
        dataset = self.root / "dataset.jsonl"
        self.write_jsonl(dataset, [{"id": "alpha"}])
        policy_target = self.root / "policy-target.json"
        policy_target.write_text(
            json.dumps(
                {
                    "namespace": "datajig",
                    "schema_version": 1,
                    "adapter": "jsonl",
                    "mode": "full",
                    "fields": {},
                }
            )
        )
        policy = self.root / "policy.json"
        policy.symlink_to(policy_target)

        self.assert_cli_error(
            "WORKSPACE_ERROR",
            "init",
            dataset,
            "--id-field",
            "id",
            "--state",
            self.state,
            "--policy",
            policy,
        )
        self.assertFalse(self.state.exists())
        self.assert_no_staging_artifacts()


if __name__ == "__main__":
    import unittest

    unittest.main()
