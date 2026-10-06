from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

from hatchling.builders.config import BuilderConfig
from hatchling.builders.hooks.plugin.interface import BuildHookInterface

_PLATFORM_TAGS = {
    "x86_64-unknown-linux-gnu": "linux_x86_64",
    "aarch64-unknown-linux-gnu": "linux_aarch64",
    "x86_64-apple-darwin": "macosx_10_12_x86_64",
    "aarch64-apple-darwin": "macosx_11_0_arm64",
}

_MACOS_DEPLOYMENT_TARGETS = {
    "x86_64-apple-darwin": "10.12",
    "aarch64-apple-darwin": "11.0",
}


class CustomBuildHook(BuildHookInterface[BuilderConfig]):
    """Build and bundle the authoritative Rust CLI in release wheels."""

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        if version == "editable":
            return

        target = os.environ.get("DATAJIG_BUILD_TARGET")
        if target not in _PLATFORM_TAGS:
            supported = ", ".join(sorted(_PLATFORM_TAGS))
            raise RuntimeError(
                "DATAJIG_BUILD_TARGET must name a supported release target: " + supported
            )

        deployment_target = _MACOS_DEPLOYMENT_TARGETS.get(target)
        if (
            deployment_target is not None
            and os.environ.get("MACOSX_DEPLOYMENT_TARGET") != deployment_target
        ):
            raise RuntimeError(f"MACOSX_DEPLOYMENT_TARGET must be {deployment_target} for {target}")

        target_directory = Path(self.root, "rust", "target", "datajig-wheel")
        command = [
            "cargo",
            "build",
            "--release",
            "--locked",
            "--manifest-path",
            "rust/datajig-core/Cargo.toml",
            "--target-dir",
            str(target_directory),
            "--target",
            target,
            "--bin",
            "datajig-core",
        ]
        subprocess.run(command, cwd=self.root, check=True)

        executable = "datajig-core.exe" if "windows" in target else "datajig-core"
        artifact = target_directory / target / "release" / executable
        if not artifact.is_file():
            raise RuntimeError(f"Cargo did not produce the expected artifact: {artifact}")

        build_data["force_include"][str(artifact)] = f"datajig/bin/{executable}"
        build_data["tag"] = f"py3-none-{_PLATFORM_TAGS[target]}"
