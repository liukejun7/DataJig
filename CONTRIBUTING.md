# Contributing to DataJig

DataJig is experimental software. Contributions are welcome, especially small,
focused changes that preserve deterministic behavior and keep artifact and CLI
contracts explicit.

## Development environment

Use Python 3.11 or newer and Rust 1.85. Install the Python development
dependencies and build the Rust workspace from the repository root:

```bash
python -m pip install -e '.[dev]'
cargo build --locked --manifest-path rust/Cargo.toml --workspace
```

## Before submitting a change

Run the same checks as CI from the repository root:

```bash
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo clippy --locked --manifest-path rust/Cargo.toml --workspace --all-targets -- -D warnings
cargo test --locked --manifest-path rust/Cargo.toml --workspace
cargo build --locked --manifest-path rust/Cargo.toml --workspace
python -m ruff check .
python -m mypy src
python -m compileall -q src
```

For the CLI integration suite, first build the debug binary with the command
above. Set `DATAJIG_NATIVE` to `$PWD/rust/target/debug/datajig-core` and
`PYTHONPATH` to `src`, then run:

```bash
python -m unittest discover -s tests -v
```

Tests should be deterministic: avoid network dependencies, ambient state,
timing assumptions, and assertions that depend on unordered output. Add or
update focused tests for user-visible behavior and failure paths.

## Change scope

- Keep each change small enough to review as one coherent unit.
- Preserve documented privacy, integrity, and fail-closed behavior.
- Update user-facing documentation when a command or artifact contract changes.
- Do not commit generated artifacts or local state, including `rust/target/`,
  `build/`, `dist/`, caches, `*.egg-info/`, or `.datajig/` workspaces.

In a pull request, explain the user-visible effect, the tests run, and any
compatibility or migration considerations.
