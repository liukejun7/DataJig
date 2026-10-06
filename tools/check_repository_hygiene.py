#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path, PurePosixPath

REQUIRED_AUTHOR_EMAIL = "liukj7@gmail.com"
FORBIDDEN_EMAIL = bytes.fromhex(
    "6461776e6b69737365724064722e636f6d"
).decode("ascii")
FORBIDDEN_ROOTS = {
    ".benchmarks",
    ".datajig",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".superpowers",
    ".venv",
    "build",
    "dist",
    "docs",
}


def git(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", *arguments],
        check=check,
        capture_output=True,
    )


def forbidden_path(path: str) -> bool:
    parts = PurePosixPath(path).parts
    if not parts:
        return False
    if parts[0] in FORBIDDEN_ROOTS or path == "uv.lock":
        return True
    lowered = path.lower()
    if path.startswith("rust/target/") or path.endswith((".pyc", ".pyo", ".whl")):
        return True
    return (
        "__pycache__" in parts
        or any(part.endswith(".egg-info") for part in parts)
        or "implementation-plan" in lowered
        or lowered.endswith("/progress.md")
    )


def configured_email() -> str:
    completed = git("config", "--get", "user.email", check=False)
    return completed.stdout.decode("utf-8", errors="replace").strip()


def check_identity(errors: list[str]) -> None:
    actual = configured_email()
    if actual != REQUIRED_AUTHOR_EMAIL:
        errors.append(
            f"repository author email must be {REQUIRED_AUTHOR_EMAIL}; found {actual or '<unset>'}"
        )


def check_staged(errors: list[str]) -> None:
    check_identity(errors)
    names = git("diff", "--cached", "--name-only", "--diff-filter=ACMR", "-z").stdout
    for raw_path in names.split(b"\0"):
        if not raw_path:
            continue
        path = raw_path.decode("utf-8", errors="surrogateescape")
        if forbidden_path(path):
            errors.append(f"forbidden staged path: {path}")
            continue
        payload = git("show", f":{path}", check=False)
        if payload.returncode == 0 and FORBIDDEN_EMAIL.encode() in payload.stdout.lower():
            errors.append(f"forbidden email in staged file: {path}")


def check_message(path: Path, errors: list[str]) -> None:
    check_identity(errors)
    try:
        payload = path.read_bytes()
    except OSError as error:
        errors.append(f"cannot read commit message: {error}")
        return
    if FORBIDDEN_EMAIL.encode() in payload.lower():
        errors.append("forbidden email in commit message")


def check_history(errors: list[str]) -> None:
    authors = git("log", "--format=%ae", "HEAD").stdout.decode().splitlines()
    for email in sorted(set(authors)):
        if email != REQUIRED_AUTHOR_EMAIL:
            errors.append(f"unexpected author email in history: {email}")
    metadata = git("log", "--format=%ae%n%ce%n%B", "HEAD").stdout.lower()
    if FORBIDDEN_EMAIL.encode() in metadata:
        errors.append("forbidden email in commit metadata or message")
    objects = git("rev-list", "--objects", "HEAD").stdout.decode(
        "utf-8", errors="surrogateescape"
    )
    for line in objects.splitlines():
        _, separator, path = line.partition(" ")
        if separator and forbidden_path(path):
            errors.append(f"forbidden path in reachable history: {path}")
            break


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enforce DataJig repository hygiene")
    parser.add_argument("--mode", choices=("staged", "message", "history"), required=True)
    parser.add_argument("--message-file", type=Path)
    arguments = parser.parse_args()
    if arguments.mode == "message" and arguments.message_file is None:
        parser.error("--message-file is required in message mode")
    return arguments


def main() -> int:
    arguments = parse_arguments()
    errors: list[str] = []
    if arguments.mode == "staged":
        check_staged(errors)
    elif arguments.mode == "message":
        check_message(arguments.message_file, errors)
    else:
        check_history(errors)
    if errors:
        for error in errors:
            print(f"repository hygiene: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
