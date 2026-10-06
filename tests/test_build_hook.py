from __future__ import annotations

import os
import unittest
from unittest import mock

from hatch_build import _resolve_build_target


class BuildTargetTests(unittest.TestCase):
    def test_supported_native_host_is_the_safe_local_default(self) -> None:
        with (
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch("hatch_build.platform.system", return_value="Linux"),
            mock.patch("hatch_build.platform.machine", return_value="x86_64"),
        ):
            self.assertEqual("x86_64-unknown-linux-gnu", _resolve_build_target())

    def test_explicit_release_target_remains_authoritative(self) -> None:
        with mock.patch.dict(
            os.environ, {"DATAJIG_BUILD_TARGET": "aarch64-apple-darwin"}, clear=True
        ):
            self.assertEqual("aarch64-apple-darwin", _resolve_build_target())

    def test_unknown_host_fails_instead_of_mislabelling_a_wheel(self) -> None:
        with (
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch("hatch_build.platform.system", return_value="Windows"),
            mock.patch("hatch_build.platform.machine", return_value="AMD64"),
            self.assertRaisesRegex(RuntimeError, "DATAJIG_BUILD_TARGET"),
        ):
            _resolve_build_target()


if __name__ == "__main__":
    unittest.main()
