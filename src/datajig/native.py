from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

NATIVE_MANIFEST_COMMANDS = frozenset({"snapshot", "snapshot-diff", "snapshot-info"})
NATIVE_INVENTORY_COMMANDS = frozenset({"inventory"})
NATIVE_RECORD_COMMANDS = frozenset({"inspect", "record-diff"})
NATIVE_TRAINING_COMMANDS = frozenset({"export", "export-info", "view-check"})
NATIVE_CONSUMPTION_COMMANDS = frozenset({"consume-info", "consume-plan"})
NATIVE_HF_IMPORT_COMMANDS = frozenset({"hf-import-apply", "hf-import-plan"})
NATIVE_PREPARE_COMMANDS = frozenset({"prepare-apply", "prepare-plan"})
NATIVE_TRANSFORM_COMMANDS = frozenset(
    {"transform-apply", "transform-info", "transform-plan"}
)
NATIVE_REPOSITORY_COMMANDS = frozenset({"repository-check", "repository-install"})
NATIVE_REPORT_COMMANDS = frozenset({"explain", "finding", "findings", "review"})
NATIVE_WORKSPACE_COMMANDS = frozenset(
    {
        "changeset-begin",
        "changeset-stage",
        "check",
        "export",
        "init",
        "locate",
        "patch-apply",
        "patch-draft",
        "patch-preview",
        "patch-undo",
        "log",
        "materialize",
        "review-plan",
        "seal",
        "status",
        "tutorial",
        "view-check",
    }
)
NATIVE_CONTROL_COMMANDS = frozenset(
    {"agent-skill", "artifact-schema", "capabilities", "describe"}
)
NATIVE_COMMANDS = (
    NATIVE_CONTROL_COMMANDS
    | NATIVE_INVENTORY_COMMANDS
    | NATIVE_RECORD_COMMANDS
    | NATIVE_MANIFEST_COMMANDS
    | NATIVE_REPORT_COMMANDS
    | NATIVE_TRAINING_COMMANDS
    | NATIVE_CONSUMPTION_COMMANDS
    | NATIVE_HF_IMPORT_COMMANDS
    | NATIVE_PREPARE_COMMANDS
    | NATIVE_TRANSFORM_COMMANDS
    | NATIVE_REPOSITORY_COMMANDS
    | NATIVE_WORKSPACE_COMMANDS
)


class NativeBackendUnavailableError(RuntimeError):
    """Raised when the authoritative native backend cannot be executed."""


class TransformProviderUnavailableError(RuntimeError):
    """Raised when an execution command cannot load the pinned optional provider."""

    remediation = "pip install 'datajig[duckdb]'"

    def __init__(self, message: str, *, code: str = "PROVIDER_UNAVAILABLE") -> None:
        super().__init__(message)
        self.code = code


def run_native(args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    binary = _resolve_native_binary()
    command = args[0] if args else ""
    native_environment = os.environ.copy()
    native_environment.pop("_DATAJIG_PROVIDER_PYTHON", None)
    if command in NATIVE_TRANSFORM_COMMANDS:
        native_environment["_DATAJIG_PROVIDER_PYTHON"] = os.path.abspath(sys.executable)
    requests_help = any(item in {"-h", "--help"} for item in args[1:])
    if command == "transform-plan" and not requests_help:
        _verify_transform_provider()
    try:
        _verify_native(binary, args, native_environment)
        return subprocess.run(
            [str(binary), *args],
            check=False,
            capture_output=True,
            encoding="utf-8",
            env=native_environment,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise NativeBackendUnavailableError(
            "the Rust backend could not be started; install datajig-core or set "
            "DATAJIG_NATIVE to an executable"
        ) from exc


def _resolve_native_binary() -> Path:
    executable = "datajig-core.exe" if os.name == "nt" else "datajig-core"
    override = os.environ.get("DATAJIG_NATIVE")
    if override is not None:
        candidate = Path(override).expanduser()
        if _is_executable(candidate):
            return candidate
        raise NativeBackendUnavailableError(
            "DATAJIG_NATIVE does not name an executable Rust backend"
        )

    package_candidate = Path(__file__).resolve().parent / "bin" / executable
    if _is_executable(package_candidate):
        return package_candidate

    repository = Path(__file__).resolve().parents[2]
    for profile in ("release", "debug"):
        development_candidate = repository / "rust" / "target" / profile / executable
        if _is_executable(development_candidate):
            return development_candidate

    discovered = shutil.which("datajig-core")
    if discovered is not None:
        return Path(discovered)
    raise NativeBackendUnavailableError(
        "the Rust backend is required; install datajig-core or set DATAJIG_NATIVE"
    )


def _is_executable(path: Path) -> bool:
    return path.is_file() and os.access(path, os.X_OK)


def _verify_transform_provider() -> None:
    try:
        from datajig.providers.duckdb import ProviderError, _probe

        _probe()
    except (ImportError, ModuleNotFoundError) as exc:
        raise TransformProviderUnavailableError(
            "DuckDB transform support is not installed. Install it with: "
            "pip install 'datajig[duckdb]'"
        ) from exc
    except ProviderError as exc:
        raise TransformProviderUnavailableError(
            f"{exc.message} {exc.remediation}", code=exc.code
        ) from exc


def _verify_native(
    binary: Path, args: Sequence[str], native_environment: dict[str, str]
) -> None:
    command = args[0] if args else ""
    verification_environment = native_environment.copy()
    verification_environment.pop("_DATAJIG_PROVIDER_PYTHON", None)
    completed = subprocess.run(
        [str(binary), "capabilities"],
        check=False,
        capture_output=True,
        timeout=5,
        env=verification_environment,
    )
    try:
        capabilities = json.loads(completed.stdout.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise NativeBackendUnavailableError(
            "the configured native backend returned an invalid capability document"
        ) from exc
    commands = capabilities.get("commands") if isinstance(capabilities, dict) else None
    tool = capabilities.get("tool") if isinstance(capabilities, dict) else None
    schema_versions = (
        capabilities.get("manifest_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    report_schema_versions = (
        capabilities.get("report_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    inventory_schema_versions = (
        capabilities.get("inventory_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    jsonl_inspection_schema_versions = (
        capabilities.get("jsonl_inspection_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    record_diff_schema_versions = (
        capabilities.get("record_diff_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    workspace_schema_versions = (
        capabilities.get("workspace_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    remediation_plan_schema_versions = (
        capabilities.get("remediation_plan_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    revision_schema_versions = (
        capabilities.get("revision_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    changeset_schema_versions = (
        capabilities.get("changeset_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    record_state_schema_versions = (
        capabilities.get("jsonl_record_state_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    record_page_schema_versions = (
        capabilities.get("jsonl_record_page_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    quality_policy_schema_versions = (
        capabilities.get("jsonl_quality_policy_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    patch_schema_versions = (
        capabilities.get("jsonl_patch_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    patch_receipt_schema_versions = (
        capabilities.get("jsonl_patch_receipt_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    artifact_schema_versions = (
        capabilities.get("artifact_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    repository_integration_schema_versions = (
        capabilities.get("repository_integration_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    training_bundle_schema_versions = (
        capabilities.get("training_bundle_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    consumer_plan_schema_versions = (
        capabilities.get("training_consumer_plan_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    consumption_plan_schema_versions = (
        capabilities.get("training_consumption_plan_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    consumption_receipt_schema_versions = (
        capabilities.get("training_consumption_receipt_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    subset_recipe_schema_versions = (
        capabilities.get("jsonl_subset_recipe_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    prepare_recipe_schema_versions = (
        capabilities.get("prepare_recipe_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    hf_import_plan_schema_versions = (
        capabilities.get("hf_import_plan_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    hf_import_receipt_schema_versions = (
        capabilities.get("hf_import_receipt_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    transform_plan_schema_versions = (
        capabilities.get("transform_plan_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    transform_receipt_schema_versions = (
        capabilities.get("transform_receipt_schema_versions")
        if isinstance(capabilities, dict)
        else None
    )
    transform_provider_protocol_versions = (
        capabilities.get("transform_provider_protocol_versions")
        if isinstance(capabilities, dict)
        else None
    )
    transform_source_formats = (
        capabilities.get("transform_source_formats")
        if isinstance(capabilities, dict)
        else None
    )
    features = capabilities.get("features") if isinstance(capabilities, dict) else None
    needs_changesets = command in {"changeset-begin", "changeset-stage"} or any(
        item in {"--change", "--changeset"} for item in args[1:]
    )
    needs_historical_export = command == "capabilities" or (
        command == "export" and "--revision" in args[1:]
    )
    needs_consumer_plan = command == "capabilities" or (
        command == "export-info"
        and any(
            item
            in {
                "--consumer-plan",
                "--expect-bundle",
                "--expect-revision",
                "--require-assurance",
                "--split",
            }
            for item in args[1:]
        )
    )
    if (
        completed.returncode != 0
        or not isinstance(capabilities, dict)
        or type(capabilities.get("agent_api_version")) is not int
        or capabilities.get("agent_api_version") != 1
        or capabilities.get("backend") != "rust"
        or capabilities.get("identity_namespace") != "datajig-v1"
        or not isinstance(tool, dict)
        or tool.get("name") != "datajig"
        or not isinstance(commands, list)
        or not all(isinstance(item, str) for item in commands)
        or command not in commands
        or (
            command in NATIVE_INVENTORY_COMMANDS
            | NATIVE_WORKSPACE_COMMANDS
            | {"capabilities"}
            and (
                not isinstance(inventory_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in inventory_schema_versions
                )
            )
        )
        or (
            command in {"inspect", "capabilities"}
            and (
                not isinstance(jsonl_inspection_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in jsonl_inspection_schema_versions
                )
            )
        )
        or (
            command in {"record-diff", "capabilities"}
            and (
                not isinstance(record_diff_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in record_diff_schema_versions
                )
            )
        )
        or (
            command in NATIVE_MANIFEST_COMMANDS | {"capabilities"}
            and (
                not isinstance(schema_versions, list)
                or not any(type(item) is int and item == 1 for item in schema_versions)
            )
        )
        or (
            command in NATIVE_REPORT_COMMANDS
            | NATIVE_WORKSPACE_COMMANDS
            | {"capabilities"}
            and (
                not isinstance(report_schema_versions, list)
                or not any(
                    type(item) is int and item == 1 for item in report_schema_versions
                )
            )
        )
        or (
            command in NATIVE_WORKSPACE_COMMANDS | {"capabilities"}
            and (
                not isinstance(workspace_schema_versions, list)
                or not any(
                    type(item) is int and item == 2
                    for item in workspace_schema_versions
                )
                or not any(
                    type(item) is int and item == 3
                    for item in workspace_schema_versions
                )
                or not any(
                    type(item) is int and item == 4
                    for item in workspace_schema_versions
                )
                or not isinstance(features, dict)
                or features.get("jsonl_workspace") is not True
                or features.get("jsonl_quality_policy") is not True
                or not isinstance(record_state_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in record_state_schema_versions
                )
                or not isinstance(record_page_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in record_page_schema_versions
                )
                or not isinstance(quality_policy_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in quality_policy_schema_versions
                )
            )
        )
        or (
            command in {"locate", "capabilities"}
            and (
                not isinstance(features, dict)
                or features.get("actionable_evidence") is not True
            )
        )
        or (
            command in {"artifact-schema", "capabilities"}
            and (
                not isinstance(artifact_schema_versions, list)
                or not any(
                    type(item) is int and item == 1 for item in artifact_schema_versions
                )
            )
        )
        or (
            command in NATIVE_REPOSITORY_COMMANDS | {"capabilities"}
            and (
                not isinstance(repository_integration_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in repository_integration_schema_versions
                )
                or not isinstance(features, dict)
                or features.get("repository_managed_agent_ci") is not True
            )
        )
        or (
            command in {"patch-apply", "patch-draft", "patch-preview", "capabilities"}
            and (
                not isinstance(patch_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in patch_schema_versions
                )
                or not isinstance(features, dict)
                or features.get("guarded_jsonl_patch_preview") is not True
                or (
                    command == "patch-draft"
                    and features.get("evidence_bound_jsonl_patch_draft") is not True
                )
            )
        )
        or (
            command in {"patch-apply", "patch-undo", "capabilities"}
            and (
                not isinstance(patch_receipt_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in patch_receipt_schema_versions
                )
                or not isinstance(features, dict)
                or features.get("guarded_jsonl_patch_apply") is not True
                or features.get("privacy_safe_jsonl_patch_undo") is not True
            )
        )
        or (
            command in {"review-plan", "capabilities"}
            and (
                not isinstance(remediation_plan_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in remediation_plan_schema_versions
                )
            )
        )
        or (
            command in NATIVE_HF_IMPORT_COMMANDS | {"capabilities"}
            and (
                not isinstance(hf_import_plan_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in hf_import_plan_schema_versions
                )
                or not isinstance(hf_import_receipt_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in hf_import_receipt_schema_versions
                )
                or not isinstance(features, dict)
                or features.get("hugging_face_revision_import") is not True
            )
        )
        or (
            command in NATIVE_PREPARE_COMMANDS | {"capabilities"}
            and (
                not isinstance(prepare_recipe_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in prepare_recipe_schema_versions
                )
                or not isinstance(features, dict)
                or features.get("deterministic_data_preparation") is not True
            )
        )
        or (
            command in NATIVE_TRANSFORM_COMMANDS | {"capabilities"}
            and (
                not isinstance(transform_plan_schema_versions, list)
                or 2 not in transform_plan_schema_versions
                or not isinstance(transform_receipt_schema_versions, list)
                or 1 not in transform_receipt_schema_versions
                or not isinstance(transform_provider_protocol_versions, list)
                or 1 not in transform_provider_protocol_versions
                or transform_source_formats != ["csv", "parquet", "jsonl"]
                or not isinstance(features, dict)
                or features.get("agent_native_transforms") is not True
            )
        )
        or (
            command in NATIVE_TRAINING_COMMANDS | {"capabilities"}
            and (
                not isinstance(training_bundle_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in training_bundle_schema_versions
                )
                or not isinstance(features, dict)
                or features.get("verified_training_export") is not True
                or features.get("sealed_subset_views") is not True
                or not isinstance(subset_recipe_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in subset_recipe_schema_versions
                )
            )
        )
        or (
            command in NATIVE_CONSUMPTION_COMMANDS | {"capabilities"}
            and (
                not isinstance(consumption_plan_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in consumption_plan_schema_versions
                )
                or not isinstance(consumption_receipt_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in consumption_receipt_schema_versions
                )
                or not isinstance(features, dict)
                or features.get("training_consumption_receipts") is not True
                or capabilities.get("training_consumption_claim")
                != "all_verified_split_records_crossed_adapter_boundary_at_least_once"
            )
        )
        or (
            needs_historical_export
            and (
                not isinstance(features, dict)
                or features.get("historical_revision_export") is not True
            )
        )
        or (
            needs_consumer_plan
            and (
                not isinstance(consumer_plan_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in consumer_plan_schema_versions
                )
                or not isinstance(features, dict)
                or features.get("verified_training_loader") is not True
            )
        )
        or (
            command in {"init", "log", "materialize", "seal", "status", "capabilities"}
            and (
                not isinstance(revision_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in revision_schema_versions
                )
                or not any(
                    type(item) is int and item == 2
                    for item in revision_schema_versions
                )
                or not any(
                    type(item) is int and item == 3
                    for item in revision_schema_versions
                )
            )
        )
        or (
            needs_changesets
            and (
                not isinstance(changeset_schema_versions, list)
                or not any(
                    type(item) is int and item == 1
                    for item in changeset_schema_versions
                )
                or not any(
                    type(item) is int and item == 2
                    for item in changeset_schema_versions
                )
            )
        )
    ):
        raise NativeBackendUnavailableError(
            "the configured native backend is incompatible with this Python integration"
        )
