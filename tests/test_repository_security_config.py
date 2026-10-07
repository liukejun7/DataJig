from __future__ import annotations

import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FULL_COMMIT = re.compile(r"^[0-9a-f]{40}$")


class RepositorySecurityConfigurationTests(unittest.TestCase):
    def test_every_external_action_is_pinned_to_a_full_commit(self) -> None:
        workflow_dir = ROOT / ".github" / "workflows"
        workflows = sorted(workflow_dir.glob("*.yml"))
        self.assertTrue(workflows)

        for workflow in workflows:
            lines = workflow.read_text(encoding="utf-8").splitlines()
            for line_number, line in enumerate(lines, 1):
                match = re.match(r"\s*-?\s*uses:\s*([^\s#]+)", line)
                if not match or match.group(1).startswith("./"):
                    continue
                reference = match.group(1)
                self.assertIn("@", reference, f"{workflow}:{line_number}")
                revision = reference.rsplit("@", 1)[1]
                self.assertRegex(revision, FULL_COMMIT, f"{workflow}:{line_number}")

    def test_renovate_is_bounded_and_defers_security_alerts_to_dependabot(self) -> None:
        config = json.loads((ROOT / "renovate.json").read_text(encoding="utf-8"))

        self.assertEqual("Asia/Shanghai", config["timezone"])
        self.assertLessEqual(config["prConcurrentLimit"], 5)
        self.assertEqual("7 days", config["minimumReleaseAge"])
        self.assertFalse(config["vulnerabilityAlerts"]["enabled"])
        self.assertTrue(config["lockFileMaintenance"]["enabled"])
        self.assertIn("helpers:pinGitHubActionDigests", config["extends"])

    def test_release_attests_wheels_before_any_publication(self) -> None:
        publish = (ROOT / ".github" / "workflows" / "publish.yml").read_text(
            encoding="utf-8"
        )

        self.assertIn("attestations: write", publish)
        self.assertIn("actions/attest-build-provenance@", publish)
        attestation = publish.index("actions/attest-build-provenance@")
        self.assertLess(attestation, publish.index("gh release upload"))
        self.assertLess(attestation, publish.index("pypa/gh-action-pypi-publish@"))

    def test_codeql_and_scorecard_upload_security_results(self) -> None:
        codeql = (ROOT / ".github" / "workflows" / "codeql.yml").read_text(encoding="utf-8")
        scorecard = (ROOT / ".github" / "workflows" / "scorecard.yml").read_text(
            encoding="utf-8"
        )

        self.assertIn("language: [python, rust]", codeql)
        self.assertIn("languages: ${{ matrix.language }}", codeql)
        self.assertIn("category: /language:${{ matrix.language }}", codeql)
        self.assertIn("security-events: write", codeql)
        self.assertIn("github/codeql-action/analyze@", codeql)
        self.assertIn("ossf/scorecard-action@", scorecard)
        self.assertIn("github/codeql-action/upload-sarif@", scorecard)
        self.assertIn("id-token: write", scorecard)


if __name__ == "__main__":
    unittest.main()
