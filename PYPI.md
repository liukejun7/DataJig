# DataJig

**The agent-native data control plane for reproducible model training.**

DataJig gives AI agents a safe, auditable path from raw data to model training.
It wraps data work in explicit plans, deterministic identities, semantic checks,
immutable revisions, and evidence of which records crossed a verified training
adapter boundary.

```text
raw source -> pin -> prepare/transform -> review -> seal -> export -> train
                `------ content-addressed evidence -------'  `-> receipt
```

DataJig is local-first and ships as a Python wheel containing its Rust core. It
supports CSV, flat Parquet, keyed JSONL, and ImageFolder workflows.

## Install

```bash
python -m pip install datajig
datajig --version
datajig capabilities
datajig tutorial ./datajig-tutorial
```

Add the exactly pinned DuckDB provider for bounded joins, projections, filters,
and aggregations:

```bash
python -m pip install 'datajig[duckdb]'
```

## Where it fits

DataJig does not replace Git, DVC, Oxen, lakeFS, DuckDB, Polars, Spark,
PyTorch, or Hugging Face. It sits between those systems as an agent-facing
control plane:

- pin exact upstream bytes and preserve provenance;
- authorize bounded preparation and SQL transform plans;
- scope agent edits and reject stale review evidence;
- seal content-addressed, immutable dataset revisions;
- export deterministic subsets, splits, and training shards;
- verify records at the training adapter boundary and write an at-least-once
  receipt for that boundary.

Every task, candidate, review, revision, subset, transform, and training bundle
receives a deterministic identity. If data changes after review, a plan drifts,
or an agent touches bytes outside its declared task, DataJig fails closed.

## Agent-native interface

Commands return bounded JSON with actionable `next_actions`. Discovery exposes
a deterministic `agent_contract_id`, machine-readable command descriptors, and
artifact schemas. `repository-install` can install a version-matched Agent Skill,
pre-commit guard, SHA-pinned GitHub Actions workflow, and content-addressed lock
into another repository.

```bash
datajig repository-install --root .
datajig repository-check --root .
```

## Typical workflow

```bash
datajig inspect data/papers.parquet --id-field paper_id
datajig prepare-plan data/papers.parquet \
  --recipe recipes/papers.prepare.json \
  --output data/papers.jsonl \
  --plan artifacts/papers.prepare.plan.json
datajig prepare-apply artifacts/papers.prepare.plan.json \
  --accept-plan prep_...
datajig init data/papers.jsonl --id-field paper_id
```

The optional DuckDB provider follows the same plan-and-accept pattern. SQL is
authorized by the Rust core before execution, input aliases are staged privately,
resources are bounded, and canonical JSONL output is independently verified.

## Project links

- Repository: https://github.com/liukejun7/DataJig
- Releases: https://github.com/liukejun7/DataJig/releases
- Changelog: https://github.com/liukejun7/DataJig/blob/main/CHANGELOG.md
- Security policy: https://github.com/liukejun7/DataJig/blob/main/SECURITY.md
- License: Apache-2.0
