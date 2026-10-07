#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import unittest
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
# Direct script execution puts ``tools/`` rather than the repository root on
# sys.path. Make the checked-out test package importable exactly as it is in CI.
sys.path.insert(0, str(REPOSITORY_ROOT))
SUITES = {
    "core-workflows": (
        "tests.test_hf_import_cli",
        "tests.test_historical_export",
        "tests.test_patch_transactions",
        "tests.test_prepare_directories",
        "tests.test_repository_cli",
        "tests.test_run_control",
        "tests.test_run_security_regressions",
        "tests.test_small_shards",
        "tests.test_trust_boundaries",
        "tests.test_workspace_workflows",
    ),
    "transform-pipeline": (
        "tests.test_duckdb_provider",
        "tests.test_end_to_end_journey",
        "tests.test_transform_cli",
        "tests.test_pipeline_plan",
        "tests.test_pipeline_apply",
        "tests.test_pipeline_observability",
    ),
    "run-benchmark": (
        "tests.test_run_m2",
        "tests.test_run_m3",
        "tests.test_run_m4",
        "tests.test_run_unsupported_benchmark",
    ),
    "training-release": (
        "tests.test_build_hook",
        "tests.test_cli_discovery",
        "tests.test_repository_hygiene",
        "tests.test_repository_security_config",
        "tests.test_training_consumption",
        "tests.test_tutorial",
    ),
}

RUN_BENCHMARK_SUPPORTED = (
    "tests.test_run_m2",
    "tests.test_run_m3",
    "tests.test_run_m4",
)
RUN_BENCHMARK_UNSUPPORTED = ("tests.test_run_unsupported_benchmark",)


def validate_suite_coverage() -> None:
    expected = {
        f"tests.{path.stem}" for path in (REPOSITORY_ROOT / "tests").glob("test_*.py")
    }
    assigned = [module for modules in SUITES.values() for module in modules]
    duplicates = sorted({module for module in assigned if assigned.count(module) > 1})
    missing = sorted(expected - set(assigned))
    unknown = sorted(set(assigned) - expected)
    if duplicates or missing or unknown:
        raise SystemExit(
            "invalid CI suite coverage: "
            f"duplicates={duplicates}, missing={missing}, unknown={unknown}"
        )
    loader = unittest.defaultTestLoader
    supported = loader.loadTestsFromNames(RUN_BENCHMARK_SUPPORTED).countTestCases()
    unsupported = loader.loadTestsFromNames(RUN_BENCHMARK_UNSUPPORTED).countTestCases()
    if (supported, unsupported) != (16, 4):
        raise SystemExit(
            "invalid DataJig 0.9 run benchmark: "
            f"supported={supported}/16, unsupported={unsupported}/4"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one complete DataJig CI test shard")
    parser.add_argument("suite", nargs="?", choices=sorted(SUITES))
    parser.add_argument("--list", action="store_true", dest="list_suites")
    arguments = parser.parse_args()
    validate_suite_coverage()
    if arguments.list_suites:
        print(json.dumps(SUITES, sort_keys=True, separators=(",", ":")))
        return 0
    if arguments.suite is None:
        parser.error("suite is required unless --list is used")
    selected = unittest.defaultTestLoader.loadTestsFromNames(SUITES[arguments.suite])
    result = unittest.TextTestRunner(verbosity=2).run(selected)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
