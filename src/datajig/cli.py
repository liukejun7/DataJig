from __future__ import annotations

import argparse
import json
import sys
import tomllib
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Never

from datajig.native import (
    NATIVE_COMMANDS,
    NativeBackendUnavailableError,
    run_native,
)

if TYPE_CHECKING:
    from datajig.models import ReviewStatus
    from datajig.policy import PolicyConfig

AGENT_COMMANDS = NATIVE_COMMANDS


class CliArgumentError(ValueError):
    """Raised instead of exiting so agent commands can return JSON errors."""


class DataJigArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise CliArgumentError(message)


def build_parser() -> argparse.ArgumentParser:
    from datajig.models import Severity

    parser = DataJigArgumentParser(prog="datajig")
    subparsers = parser.add_subparsers(dest="command", required=True)
    compare_parser = subparsers.add_parser("compare", help="compare two dataset snapshots")
    compare_parser.add_argument("baseline", type=Path)
    compare_parser.add_argument("candidate", type=Path)
    compare_parser.add_argument("--layout", choices=("imagefolder",), default="imagefolder")
    compare_parser.add_argument("--json", dest="json_path", type=Path)
    compare_parser.add_argument("--output", type=Path, help="write a self-contained HTML report")
    compare_parser.add_argument("--workers", type=int, default=1)
    compare_parser.add_argument("--phash-threshold", type=int, default=6)
    cache_group = compare_parser.add_mutually_exclusive_group()
    cache_group.add_argument("--cache", type=Path)
    cache_group.add_argument("--no-cache", action="store_true")
    compare_parser.add_argument("--no-thumbnails", action="store_true")
    compare_parser.add_argument("--policy", type=Path)

    agent_skill_parser = subparsers.add_parser(
        "agent-skill", help="generate a version-matched Agent Skill"
    )
    agent_skill_parser.add_argument("--output", type=Path, required=True)

    repository_install_parser = subparsers.add_parser(
        "repository-install",
        help="install the repository-local DataJig Agent and CI contract",
    )
    repository_install_parser.add_argument("--root", type=Path, default=Path("."))
    repository_install_parser.add_argument("--state", type=Path, action="append", default=[])
    repository_install_parser.add_argument("--no-hook", action="store_true")
    repository_install_parser.add_argument("--no-github-actions", action="store_true")

    repository_check_parser = subparsers.add_parser(
        "repository-check",
        help="verify repository integration and bound workspace readiness",
    )
    repository_check_parser.add_argument("--root", type=Path, default=Path("."))
    repository_check_parser.add_argument("--ci", action="store_true")

    artifact_schema_parser = subparsers.add_parser(
        "artifact-schema", help="return a machine-readable DataJig artifact contract"
    )
    artifact_schema_parser.add_argument(
        "artifact",
        nargs="?",
        choices=(
            "jsonl-field-patch",
            "prepare-recipe",
            "repository-integration",
            "subset-view",
            "training-consumption-plan",
            "training-consumption-receipt",
        ),
    )

    subparsers.add_parser("capabilities", help="describe the machine interface")

    describe_parser = subparsers.add_parser(
        "describe", help="describe native command contracts"
    )
    describe_parser.add_argument("native_command", nargs="?")

    tutorial_parser = subparsers.add_parser(
        "tutorial", help="run a complete verified example workflow"
    )
    tutorial_parser.add_argument("output", type=Path)

    init_parser = subparsers.add_parser(
        "init", help="track a dataset and record its last-good baseline"
    )
    init_parser.add_argument("dataset", type=Path)
    init_parser.add_argument("--id-field")
    init_parser.add_argument("--policy", type=Path)
    init_parser.add_argument("--state", type=Path, default=Path(".datajig"))
    init_parser.add_argument("--threads", "--workers", type=int, default=1)

    export_parser = subparsers.add_parser(
        "export", help="export a clean sealed JSONL revision as training shards"
    )
    export_parser.add_argument("--state", type=Path, default=Path(".datajig"))
    export_parser.add_argument("--output", type=Path, required=True)
    export_parser.add_argument("--view", type=Path)
    export_parser.add_argument("--revision")
    export_parser.add_argument("--seed", default="datajig-v1")
    export_parser.add_argument("--split", action="append", required=True)
    export_parser.add_argument("--max-shard-records", type=int, default=10_000)
    export_parser.add_argument("--max-shard-bytes", type=int, default=268_435_456)

    export_info_parser = subparsers.add_parser(
        "export-info", help="inspect or verify a portable training bundle"
    )
    export_info_parser.add_argument("manifest", type=Path)
    export_info_parser.add_argument("--verify", action="store_true")
    export_info_parser.add_argument("--consumer-plan", action="store_true")
    export_info_parser.add_argument("--expect-bundle")
    export_info_parser.add_argument("--expect-revision")
    export_info_parser.add_argument(
        "--require-assurance", choices=("structural", "quality_policy")
    )
    export_info_parser.add_argument("--split")

    consume_plan_parser = subparsers.add_parser(
        "consume-plan", help="plan one verified adapter-boundary consumption run"
    )
    consume_plan_parser.add_argument("manifest", type=Path)
    consume_plan_parser.add_argument("--split", required=True)
    consume_plan_parser.add_argument(
        "--consumer", choices=("python", "pytorch", "huggingface"), required=True
    )
    consume_plan_parser.add_argument("--run-id", required=True)
    consume_plan_parser.add_argument("--output", type=Path, required=True)
    consume_plan_parser.add_argument("--plan", type=Path, required=True)

    consume_info_parser = subparsers.add_parser(
        "consume-info", help="verify one accepted training consumption plan"
    )
    consume_info_parser.add_argument("plan", type=Path)
    consume_info_parser.add_argument("--verify", action="store_true", required=True)
    consume_info_parser.add_argument("--accept-plan", required=True)

    view_check_parser = subparsers.add_parser(
        "view-check", help="resolve a sealed subset recipe against clean JSONL HEAD"
    )
    view_check_parser.add_argument("--state", type=Path, default=Path(".datajig"))
    view_check_parser.add_argument("--recipe", type=Path, required=True)

    check_parser = subparsers.add_parser(
        "check", help="review the tracked dataset against last-good"
    )
    check_parser.add_argument("--state", type=Path, default=Path(".datajig"))
    check_parser.add_argument("--threads", "--workers", type=int, default=1)
    check_parser.add_argument("--phash-threshold", type=int, default=6)
    check_parser.add_argument("--change")
    check_parser.add_argument("--changeset")

    changeset_begin_parser = subparsers.add_parser(
        "changeset-begin", help="declare an Agent task against clean dataset HEAD"
    )
    changeset_begin_parser.add_argument("--state", type=Path, default=Path(".datajig"))
    changeset_begin_parser.add_argument("--threads", "--workers", type=int, default=1)
    changeset_begin_parser.add_argument("--intent", required=True)
    changeset_begin_parser.add_argument("--task-id", required=True)
    changeset_begin_parser.add_argument("--actor-kind", default="agent")

    changeset_stage_parser = subparsers.add_parser(
        "changeset-stage", help="capture a full-dataset staged changeset"
    )
    changeset_stage_parser.add_argument("--state", type=Path, default=Path(".datajig"))
    changeset_stage_parser.add_argument("--threads", "--workers", type=int, default=1)
    changeset_stage_parser.add_argument("--change", required=True)

    plan_parser = subparsers.add_parser(
        "plan", help="turn the latest review into an Agent action plan"
    )
    plan_parser.add_argument("--state", type=Path, default=Path(".datajig"))
    plan_parser.add_argument("--change")
    plan_parser.add_argument("--changeset")

    log_parser = subparsers.add_parser("log", help="read dataset revision history")
    log_parser.add_argument("--state", type=Path, default=Path(".datajig"))
    log_parser.add_argument("--offset", type=int, default=0)
    log_parser.add_argument("--limit", type=int, default=50)

    materialize_parser = subparsers.add_parser(
        "materialize", help="recover the exact bytes of an immutable JSONL revision"
    )
    materialize_parser.add_argument("revision")
    materialize_parser.add_argument("--state", type=Path, default=Path(".datajig"))
    materialize_parser.add_argument("--output", type=Path, required=True)

    locate_parser = subparsers.add_parser(
        "locate", help="resolve a staged JSONL finding to current line coordinates"
    )
    locate_parser.add_argument("report", type=Path)
    locate_parser.add_argument("finding_id")
    locate_parser.add_argument("--state", type=Path, default=Path(".datajig"))
    locate_parser.add_argument("--change", required=True)
    locate_parser.add_argument("--changeset", required=True)
    locate_parser.add_argument("--offset", type=int, default=0)
    locate_parser.add_argument("--limit", type=int, default=50)

    patch_draft_parser = subparsers.add_parser(
        "patch-draft", help="create an evidence-bound JSONL field repair request"
    )
    patch_draft_parser.add_argument("report", type=Path)
    patch_draft_parser.add_argument("finding_id")
    patch_draft_parser.add_argument("--state", type=Path, default=Path(".datajig"))
    patch_draft_parser.add_argument("--change", required=True)
    patch_draft_parser.add_argument("--changeset", required=True)
    patch_draft_parser.add_argument("--record")
    replacement = patch_draft_parser.add_mutually_exclusive_group(required=True)
    replacement.add_argument("--after-json")
    replacement.add_argument("--remove", action="store_true")
    patch_draft_parser.add_argument("--output", type=Path, required=True)

    patch_preview_parser = subparsers.add_parser(
        "patch-preview", help="verify a proposed JSONL field repair before editing"
    )
    patch_preview_parser.add_argument("request", type=Path)
    patch_preview_parser.add_argument("report", type=Path)
    patch_preview_parser.add_argument("--state", type=Path, default=Path(".datajig"))
    patch_preview_parser.add_argument("--change", required=True)
    patch_preview_parser.add_argument("--changeset", required=True)

    patch_apply_parser = subparsers.add_parser(
        "patch-apply", help="atomically apply an accepted JSONL field repair"
    )
    patch_apply_parser.add_argument("request", type=Path)
    patch_apply_parser.add_argument("report", type=Path)
    patch_apply_parser.add_argument("--state", type=Path, default=Path(".datajig"))
    patch_apply_parser.add_argument("--change", required=True)
    patch_apply_parser.add_argument("--changeset", required=True)
    patch_apply_parser.add_argument("--accept-patch", required=True)

    patch_undo_parser = subparsers.add_parser(
        "patch-undo", help="restore the exact bytes replaced by a guarded patch"
    )
    patch_undo_parser.add_argument("undo_id")
    patch_undo_parser.add_argument("--state", type=Path, default=Path(".datajig"))

    hf_import_plan_parser = subparsers.add_parser(
        "hf-import-plan", help="plan selected files from an immutable Hugging Face revision"
    )
    hf_import_plan_parser.add_argument("repository")
    hf_import_plan_parser.add_argument("--revision", default="main")
    hf_import_plan_parser.add_argument("--include", action="append", default=[])
    hf_import_plan_parser.add_argument("--ignore", action="append", default=[])
    hf_import_plan_parser.add_argument("--output", type=Path, required=True)
    hf_import_plan_parser.add_argument("--plan", type=Path, required=True)

    hf_import_apply_parser = subparsers.add_parser(
        "hf-import-apply", help="apply an accepted Hugging Face import plan"
    )
    hf_import_apply_parser.add_argument("plan", type=Path)
    hf_import_apply_parser.add_argument("--accept-plan", required=True)

    prepare_plan_parser = subparsers.add_parser(
        "prepare-plan",
        help="plan deterministic CSV/Parquet/JSONL preparation from a file or import receipt",
    )
    prepare_plan_parser.add_argument("source", type=Path)
    prepare_plan_parser.add_argument("--recipe", type=Path, required=True)
    prepare_plan_parser.add_argument("--output", type=Path, required=True)
    prepare_plan_parser.add_argument("--plan", type=Path, required=True)

    prepare_apply_parser = subparsers.add_parser(
        "prepare-apply", help="apply an accepted deterministic preparation plan"
    )
    prepare_apply_parser.add_argument("plan", type=Path)
    prepare_apply_parser.add_argument("--accept-plan", required=True)

    seal_parser = subparsers.add_parser(
        "seal", help="promote a fresh passing review to last-good"
    )
    seal_parser.add_argument("--state", type=Path, default=Path(".datajig"))
    seal_parser.add_argument("--threads", "--workers", type=int, default=1)
    seal_parser.add_argument("--message", default="Accept dataset revision")
    seal_parser.add_argument("--accept-report")
    seal_parser.add_argument("--change")
    seal_parser.add_argument("--changeset")

    status_parser = subparsers.add_parser(
        "status", help="inspect current dataset and workspace identity"
    )
    status_parser.add_argument("--state", type=Path, default=Path(".datajig"))
    status_parser.add_argument("--threads", "--workers", type=int, default=1)
    status_parser.add_argument("--change")
    status_parser.add_argument("--changeset")

    inventory_parser = subparsers.add_parser(
        "inventory", help="inspect an ImageFolder dataset"
    )
    inventory_parser.add_argument("root", type=Path)
    inventory_parser.add_argument("--output", type=Path, required=True)
    inventory_parser.add_argument("--threads", "--workers", type=int, default=1)

    review_parser = subparsers.add_parser(
        "review", help="create a native semantic review artifact"
    )
    review_parser.add_argument("before_ref")
    review_parser.add_argument("after_ref")
    review_parser.add_argument("--output", type=Path, required=True)
    review_parser.add_argument("--threads", "--workers", type=int, default=1)
    review_parser.add_argument("--phash-threshold", type=int, default=6)

    findings_parser = subparsers.add_parser(
        "findings", help="list findings from a saved JSON report"
    )
    findings_parser.add_argument("report", type=Path)
    findings_parser.add_argument(
        "--severity", action="append", choices=tuple(item.value for item in Severity)
    )
    findings_parser.add_argument("--code", action="append")
    findings_parser.add_argument("--offset", type=int, default=0)
    findings_parser.add_argument("--limit", type=int, default=50)

    finding_parser = subparsers.add_parser(
        "finding", help="get one finding from a saved JSON report"
    )
    finding_parser.add_argument("report", type=Path)
    finding_parser.add_argument("finding_id")

    explain_parser = subparsers.add_parser(
        "explain", help="summarize a saved JSON report for an agent"
    )
    explain_parser.add_argument("report", type=Path)
    explain_parser.add_argument("--limit", type=int, default=10)

    snapshot_parser = subparsers.add_parser(
        "snapshot", help="create a content-addressed directory manifest"
    )
    snapshot_parser.add_argument("root", type=Path)
    snapshot_parser.add_argument("--output", type=Path, required=True)
    snapshot_parser.add_argument("--workers", type=int, default=1)

    snapshot_info_parser = subparsers.add_parser(
        "snapshot-info", help="read a snapshot manifest summary"
    )
    snapshot_info_parser.add_argument("manifest", type=Path)

    snapshot_diff_parser = subparsers.add_parser(
        "snapshot-diff", help="compare two snapshot manifests"
    )
    snapshot_diff_parser.add_argument("before", type=Path)
    snapshot_diff_parser.add_argument("after", type=Path)
    snapshot_diff_parser.add_argument("--offset", type=int, default=0)
    snapshot_diff_parser.add_argument("--limit", type=int, default=50)

    inspect_parser = subparsers.add_parser(
        "inspect", help="inspect JSONL, CSV, or Parquet without exposing values"
    )
    inspect_parser.add_argument("source", type=Path)
    inspect_parser.add_argument("--id-field", default="id")
    inspect_parser.add_argument("--delimiter", default=",")

    record_diff_parser = subparsers.add_parser(
        "record-diff", help="compare keyed JSONL records semantically"
    )
    record_diff_parser.add_argument("before", type=Path)
    record_diff_parser.add_argument("after", type=Path)
    record_diff_parser.add_argument("--id-field", default="id")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    raw_args = list(argv) if argv is not None else sys.argv[1:]
    if (
        raw_args
        and raw_args[0] in NATIVE_COMMANDS
        and any(item in {"-h", "--help"} for item in raw_args[1:])
    ):
        return _run_native_command(raw_args)
    if raw_args and raw_args[0] in NATIVE_COMMANDS:
        return _run_native_command(raw_args)
    parser = build_parser()
    try:
        args = parser.parse_args(raw_args)
    except CliArgumentError as exc:
        if raw_args and raw_args[0] in AGENT_COMMANDS:
            _print_agent_error("INVALID_ARGUMENT", str(exc))
        else:
            print(f"datajig: {exc}", file=sys.stderr)
        return 2
    return _run_compare(args)


def _run_compare(args: argparse.Namespace) -> int:
    from datajig.api import compare, validate_destination
    from datajig.policy import PolicyConfig
    from datajig.reporting.html import save_html
    from datajig.reporting.json import report_to_json

    try:
        policy = load_policy(args.policy) if args.policy else PolicyConfig()
        baseline_root = args.baseline.expanduser().resolve()
        candidate_root = args.candidate.expanduser().resolve()
        if args.json_path:
            validate_destination(args.json_path, baseline_root, candidate_root)
        if args.output:
            validate_destination(args.output, baseline_root, candidate_root)
        cache: str | Path | None = None if args.no_cache else args.cache
        report = compare(
            args.baseline,
            args.candidate,
            layout=args.layout,
            cache=cache,
            policy=policy,
            workers=args.workers,
            phash_threshold=args.phash_threshold,
        )
        rendered_json = report_to_json(report)
        if args.json_path:
            args.json_path.parent.mkdir(parents=True, exist_ok=True)
            args.json_path.write_text(rendered_json, encoding="utf-8")
        elif not args.output:
            print(rendered_json)
        if args.output:
            save_html(report, args.output, embed_thumbnails=not args.no_thumbnails)
    except (OSError, ValueError) as exc:
        print(f"datajig: {exc}", file=sys.stderr)
        return 2
    if any(finding.code == "INVALID_LAYOUT" for finding in report.findings):
        return 2
    return exit_code(report.policy.status)


def _run_native_command(raw_args: Sequence[str]) -> int:
    try:
        completed = run_native(raw_args)
    except NativeBackendUnavailableError as exc:
        _print_agent_error("NATIVE_BACKEND_UNAVAILABLE", str(exc))
        return 2
    sys.stdout.buffer.write(completed.stdout.encode("utf-8"))
    sys.stderr.buffer.write(completed.stderr.encode("utf-8"))
    return completed.returncode


def _print_agent_error(code: str, message: str) -> None:
    payload = {
        "agent_api_version": 1,
        "error": {"code": code, "message": message, "retryable": False},
    }
    print(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        file=sys.stderr,
    )


def exit_code(status: ReviewStatus) -> int:
    from datajig.models import ReviewStatus

    if status is ReviewStatus.INCOMPLETE:
        return 3
    if status is ReviewStatus.FAIL:
        return 1
    return 0


def load_policy(path: str | Path) -> PolicyConfig:
    from datajig.models import Severity
    from datajig.policy import PolicyConfig

    with Path(path).open("rb") as stream:
        payload = tomllib.load(stream)
    policy_data = payload.get("policy", {})
    if not isinstance(policy_data, dict):
        raise ValueError("policy must be a TOML table")
    severity_data = policy_data.get("severity", {})
    if not isinstance(severity_data, dict):
        raise ValueError("policy.severity must be a TOML table")
    overrides: list[tuple[str, Severity]] = []
    for code, value in severity_data.items():
        if not isinstance(code, str) or not isinstance(value, str):
            raise ValueError("policy severity entries must map strings to severity names")
        try:
            overrides.append((code, Severity(value)))
        except ValueError as exc:
            raise ValueError(f"invalid severity {value!r} for {code}") from exc
    return PolicyConfig(
        overrides=tuple(sorted(overrides)),
        label_delta_threshold=_threshold(policy_data, "label_delta_threshold", 0.1),
        media_delta_threshold=_threshold(policy_data, "media_delta_threshold", 0.1),
    )


def _threshold(data: dict[object, object], key: str, default: float) -> float:
    value = data.get(key, default)
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise ValueError(f"{key} must be a number between 0 and 1")
    return float(value)


if __name__ == "__main__":
    raise SystemExit(main())
