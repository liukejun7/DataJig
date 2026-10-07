from __future__ import annotations

from tests.cli_harness import DataJigCliTestCase


class RunControlTests(DataJigCliTestCase):
    def test_python_entrypoint_preserves_strict_plan_and_resume_contract(self) -> None:
        source = self.root / "rows.csv"
        source.write_text("id,value\n1,a\n", encoding="utf-8")
        task = "from rows.csv source-id-field id export id-field id"

        planned = self.run_cli("run", task, "--strict").payload
        self.assertEqual("run_plan_ready", planned["kind"])
        self.assertEqual("confirmation_required", planned["decision"])
        artifact = planned["artifact"]
        resumed = self.run_cli(
            "run",
            "--resume",
            artifact["attempt_id"],
            "--accept-plan",
            artifact["plan_id"],
        ).payload
        self.assertEqual("run_plan_accepted", resumed["kind"])
        self.assertEqual("ready", resumed["decision"])
        self.assertEqual(artifact["attempt_id"], resumed["artifact"]["attempt_id"])

    def test_run_errors_remain_machine_actionable(self) -> None:
        failed = self.assert_cli_error("UNSUPPORTED_TASK", "run", "连接两个数据集")

        self.assertEqual("task", failed.payload["error"]["details"]["location"])
        self.assertTrue(failed.payload["error"]["remediation"]["summary"])
        self.assertTrue(failed.payload["next_actions"])


if __name__ == "__main__":
    import unittest

    unittest.main()
