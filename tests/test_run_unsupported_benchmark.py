from __future__ import annotations

from tests.cli_harness import DataJigCliTestCase


class RunUnsupportedBenchmarkTests(DataJigCliTestCase):
    def assert_unsupported(self, task: str) -> None:
        result = self.assert_cli_error("UNSUPPORTED_TASK", "run", task)
        self.assertEqual("task", result.payload["error"]["details"]["location"])
        self.assertTrue(result.payload["error"]["details"]["suggestions"])
        self.assertTrue(result.payload["next_actions"])

    def test_remote_source_is_stably_unsupported(self) -> None:
        self.assert_unsupported(
            "from https://example.test/data.csv source-id-field id export id-field id"
        )

    def test_join_is_stably_unsupported(self) -> None:
        self.assert_unsupported(
            "from left.csv join right.csv source-id-field id export id-field id"
        )

    def test_loop_is_stably_unsupported(self) -> None:
        self.assert_unsupported(
            "from rows.csv source-id-field id loop export id-field id"
        )

    def test_unmapped_natural_language_is_stably_unsupported(self) -> None:
        self.assert_unsupported("把 data.csv 猜着处理一下")


if __name__ == "__main__":
    import unittest

    unittest.main()
