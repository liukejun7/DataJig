from __future__ import annotations

from tests.cli_harness import DataJigCliTestCase


class TutorialTests(DataJigCliTestCase):
    def test_tutorial_runs_one_complete_verified_training_workflow(self) -> None:
        output = self.root / "agent tutorial"

        result = self.run_cli("tutorial", output).payload

        self.assertEqual("tutorial_completed", result["kind"])
        self.assertEqual("ready", result["decision"])
        artifact = result["artifact"]
        self.assertEqual(
            ["init", "changeset-begin", "edit", "changeset-stage", "check", "seal", "export"],
            artifact["completed"],
        )
        self.assertTrue((output / "dataset.jsonl").is_file())
        self.assertTrue((output / ".datajig" / "workspace.json").is_file())
        manifest = output / "training-bundle" / "datajig.bundle.json"
        self.assertEqual(str(manifest.resolve()), artifact["manifest_path"])

        verified = self.run_cli(
            "export-info",
            manifest,
            "--verify",
            "--expect-bundle",
            artifact["bundle_id"],
            "--expect-revision",
            artifact["revision_id"],
        ).payload["artifact"]
        self.assertTrue(verified["verified"])
        self.assertEqual(2, verified["records"])

        status = self.run_cli("status", "--state", output / ".datajig").payload["artifact"]
        self.assertTrue(status["clean"])
        self.assertEqual(artifact["revision_id"], status["head_revision_id"])

    def test_tutorial_refuses_an_existing_output_without_modifying_it(self) -> None:
        output = self.root / "existing"
        output.mkdir()
        sentinel = output / "keep.txt"
        sentinel.write_text("keep", encoding="utf-8")

        failed = self.assert_cli_error("INVALID_ARGUMENT", "tutorial", output)

        self.assertIn("must not already exist", failed.payload["error"]["message"])
        self.assertEqual("keep", sentinel.read_text(encoding="utf-8"))
        self.assertEqual([sentinel], list(output.iterdir()))


if __name__ == "__main__":
    import unittest

    unittest.main()
