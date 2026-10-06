from __future__ import annotations

import os
import platform
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

_HOST_TARGETS = {
    ("linux", "x86_64"): "x86_64-unknown-linux-gnu",
    ("linux", "amd64"): "x86_64-unknown-linux-gnu",
    ("linux", "aarch64"): "aarch64-unknown-linux-gnu",
    ("linux", "arm64"): "aarch64-unknown-linux-gnu",
    ("darwin", "x86_64"): "x86_64-apple-darwin",
    ("darwin", "amd64"): "x86_64-apple-darwin",
    ("darwin", "aarch64"): "aarch64-apple-darwin",
    ("darwin", "arm64"): "aarch64-apple-darwin",
}


def _resolve_build_target() -> str:
    configured = os.environ.get("DATAJIG_BUILD_TARGET")
    if configured is not None:
        if configured in _PLATFORM_TAGS:
            return configured
        supported = ", ".join(sorted(_PLATFORM_TAGS))
        raise RuntimeError(
            "DATAJIG_BUILD_TARGET must name a supported release target: " + supported
        )

    host = (platform.system().lower(), platform.machine().lower())
    inferred = _HOST_TARGETS.get(host)
    if inferred is None:
        supported = ", ".join(sorted(_PLATFORM_TAGS))
        raise RuntimeError(
            "cannot infer a safe native wheel target; set DATAJIG_BUILD_TARGET to one of: "
            + supported
        )
    return inferred


class CustomBuildHook(BuildHookInterface[BuilderConfig]):
    """Build and bundle the authoritative Rust CLI in release wheels."""

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        if version == "editable":
            return

        target_was_explicit = os.environ.get("DATAJIG_BUILD_TARGET") is not None
        target = _resolve_build_target()

        deployment_target = _MACOS_DEPLOYMENT_TARGETS.get(target)
        build_environment = os.environ.copy()
        if deployment_target is not None:
            configured_deployment = os.environ.get("MACOSX_DEPLOYMENT_TARGET")
            if configured_deployment is None and not target_was_explicit:
                build_environment["MACOSX_DEPLOYMENT_TARGET"] = deployment_target
            elif configured_deployment != deployment_target:
                raise RuntimeError(
                    f"MACOSX_DEPLOYMENT_TARGET must be {deployment_target} for {target}"
                )

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
        subprocess.run(command, cwd=self.root, check=True, env=build_environment)

        executable = "datajig-core.exe" if "windows" in target else "datajig-core"
        artifact = target_directory / target / "release" / executable
        if not artifact.is_file():
            raise RuntimeError(f"Cargo did not produce the expected artifact: {artifact}")

        build_data["force_include"][str(artifact)] = f"datajig/bin/{executable}"
        build_data["tag"] = f"py3-none-{_PLATFORM_TAGS[target]}"
