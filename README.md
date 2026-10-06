<h1 align="center">DataJig</h1>

<p align="center">
  <img src="./assets/datajig-hero.png" alt="A friendly DataJig assembly line turning raw datasets into verified training bundles" width="100%">
</p>

<p align="center">
  <strong>DataJig is an agent-native data control plane that turns raw datasets into verified, versioned, training-ready inputs.</strong>
</p>

<p align="center">
  <a href="https://pypi.org/project/datajig/"><img src="https://img.shields.io/pypi/v/datajig?label=PyPI" alt="PyPI"></a>
  <a href="https://pypi.org/project/datajig/"><img src="https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white" alt="Python 3.11+"></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/License-Apache--2.0-D22128" alt="Apache-2.0 license"></a>
</p>

<p align="center">
  <a href="https://github.com/liukejun7/DataJig/blob/main/README.zh-CN.md">Chinese README</a> ·
  <a href="https://pypi.org/project/datajig/">PyPI</a> ·
  <a href="https://github.com/liukejun7/DataJig/blob/main/ARCHITECTURE.md">Architecture</a> ·
  <a href="https://github.com/liukejun7/DataJig/blob/main/ROADMAP.md">Roadmap</a> ·
  <a href="https://github.com/liukejun7/DataJig/blob/main/CONTRIBUTING.md">Contributing</a> ·
  <a href="https://github.com/liukejun7/DataJig/blob/main/SECURITY.md">Security</a> ·
  <a href="https://github.com/liukejun7/DataJig/blob/main/CHANGELOG.md">Changelog</a>
</p>

DataJig gives AI agents a safe, auditable path from raw data to model training.
It wraps data work in explicit plans, deterministic identities, semantic checks,
immutable revisions, and evidence of which records crossed a verified training
adapter boundary.

```text
raw source → pin → prepare/transform → review → seal → export → train
                └──────── content-addressed evidence ────────┘  └→ receipt
```

Every task, candidate, review, accepted revision, subset, transform, and training
bundle receives a deterministic identity. If data changes after review, a plan
drifts, or an agent touches bytes outside its declared task, DataJig fails closed.

> Version `0.8.2` · local-first · recoverable pipelines · CSV, Parquet, JSONL, and ImageFolder

## Where DataJig fits

DataJig does not try to replace the engines around it. It makes their work safe
to delegate to agents and reproducible at the training boundary.

| Layer | Existing tools | DataJig's role |
| --- | --- | --- |
| Storage and transfer | Git, DVC, Oxen, lakeFS, object stores | Pin exact upstream bytes and preserve provenance |
| Data processing | DuckDB, Polars, Spark | Authorize bounded work; bind plans, inputs, outputs, and receipts |
| Dataset change control | Scripts and human review | Scope agent edits; check semantics; seal immutable revisions |
| Model training | PyTorch, Hugging Face, custom loaders | Deliver verified shards and record what crossed the adapter boundary |

The differentiator is the control protocol: bounded JSON, executable
`next_actions`, version-matched Agent Skills, deterministic `ds_` / `chg_` /
`rev_` identities, and evidence that can be re-verified independently.

## Try it in 60 seconds

The wheel includes the Rust core. No separate Rust toolchain is required.

```bash
python -m pip install datajig
datajig --version
datajig capabilities
datajig tutorial ./datajig-tutorial
```

The tutorial creates a tiny dataset, runs one reviewed change, seals an immutable
revision, exports a training bundle, and returns the exact verification action.
Every response is bounded JSON designed for both humans and agents.

Add the exactly pinned DuckDB provider for joins, projections, filters, and
aggregations:

```bash
python -m pip install 'datajig[duckdb]'
```

## One YAML to a training delivery

DataJig 0.8 turns the atomic workflow into one agent-safe transaction. A
pipeline binds local CSV, Parquet, or JSONL bytes, authorized SQL, the target
workspace, export settings, and per-split training consumers into one `pipe_...`
identity:

```yaml
schema_version: 1
pipeline: {name: user-agg-train, provider: duckdb}
target:
  dataset: prepared/training.jsonl
  state: workspace/.datajig
  mode: create
delivery: {output: deliveries/user-agg-train}
inputs:
  - {alias: events, path: data/events.csv, format: csv}
transform:
  sql: |
    SELECT user_id AS id, SUM(CAST(amount AS INTEGER)) AS total
    FROM events GROUP BY user_id ORDER BY id
  id_field: id
  params: []
export:
  split: [train=7, val=2, test=1]
  max_shard_records: 10000
consumption_plan:
  - {consumer: huggingface, split: train, run_id: run-2026-10-06}
```

Plan, review, and authorize the exact effect:

```bash
datajig pipeline plan --config pipeline.yaml --plan pipeline-plan.json
datajig pipeline apply pipeline-plan.json --accept-plan pipe_...
datajig pipeline info deliveries/user-agg-train/pipeline-receipt.json --verify
datajig lineage pipe_... --state workspace/.datajig --format text
```

`create` initializes a new keyed-JSONL workspace. `update` prepares a detached
revision and bundle first, then advances HEAD with compare-and-swap. A commit
marker makes the delivery visible only after every consumption plan is bound.
An eligible update may reuse the same delivery path: DataJig verifies the old
marker, receipt, bundle, dataset, base revision, and live HEAD, probes the target
filesystem, then atomically exchanges both directories and retains the verified
previous delivery as a backup. There is no non-atomic fallback.
Crashes retain a bounded journal and continue only with explicit `--resume`;
ordinary reruns never guess. Identical transformed content is a no-op revision
but still produces the requested bundle, consumer plans, and `piped_...`
receipt. Use `--config pipeline.yaml --auto-accept` only when intentionally
skipping separate plan review.

## One evidence chain, end to end

| Stage | What DataJig guarantees |
| --- | --- |
| Import | A branch or tag resolves once to an immutable Hugging Face commit and verified file set |
| Inspect | Schema, missingness, record identity, and source identity without leaking cell values |
| Prepare | Deterministic CSV, Parquet, or JSONL recipes with predicted output identity |
| Transform | Rust-authorized SQL over staged aliases with bounded DuckDB resources |
| Review | Task-scoped semantic findings and stale-evidence detection |
| Version | Content-addressed, immutable revisions with transform provenance |
| Export | Deterministic subsets, splits, and shards for training |
| Consume | Runtime verification plus an at-least-once receipt for the adapter boundary |

## Install the Agent contract in a repository

Discovery responses expose a deterministic `agent_contract_id` covering the
command catalog and artifact schemas. Install the version-matched contract,
pre-commit guard, and SHA-pinned CI workflow with one command:

```bash
datajig repository-install --root .
datajig repository-check --root .
```

Installation is idempotent and crash-recoverable. Unknown files, modified
managed assets, conflicting hooks, symlinks, hard links, and silent component
removal are rejected instead of overwritten. `repository-check` is read-only
and fails on protocol, file, or bound-workspace drift.

## Start with any supported local source

```bash
datajig inspect data/papers.jsonl --id-field paper_id
datajig inspect data/papers.csv --id-field paper_id
datajig inspect data/papers.parquet --id-field paper_id
```

`inspect` reports schema, row and missing-value counts, stable source identity,
and ID health without printing cell values or raw IDs. CSV and flat Parquet are
prepared deterministically into keyed JSONL before transactional change control.
Recursive local datasets, including Hive-style partition directories, can be
prepared as deterministic same-format shard sets. Partition path values are not
materialized as columns. XLSX and direct database or Spark table adapters are
not native yet.

Compare two keyed JSONL revisions without creating a workspace:

```bash
datajig record-diff baseline/papers.jsonl data/papers.jsonl \
  --id-field paper_id
```

`record-diff` aligns records by ID and distinguishes added, removed, modified,
and moved records. ImageFolder datasets can produce a self-contained visual
review:

```bash
datajig compare baseline/ candidate/ \
  --output review.html \
  --json review.json
```

## Import an immutable Hugging Face dataset

Resolve a dataset branch or tag once, inspect the selected repository files,
then authorize that exact plan:

```bash
mkdir -p raw artifacts

datajig hf-import-plan lhoestq/demo1 \
  --revision main \
  --include 'data/*.csv' \
  --output raw/demo1 \
  --plan artifacts/demo1.hf-plan.json

# Copy plan_id from the bounded JSON response after reviewing the plan.
datajig hf-import-apply artifacts/demo1.hf-plan.json \
  --accept-plan hfplan_...

datajig artifact-schema prepare-recipe
```

Planning resolves `main` to a 40-character commit and records the exact file
set and sizes. Apply downloads only that commit, verifies every selected file,
and writes local BLAKE3 identities to `datajig.hf-import.json` before atomically
publishing the directory. An existing output is accepted only when its receipt
and every file still match, so retries are idempotent without overwriting data.

Public, private, and gated repositories use standard Hugging Face token and
endpoint configuration. This first adapter imports files from dataset
repositories; it does not interpret Dataset Viewer configurations or generated
splits. The verified receipt can be passed directly to `prepare-plan`, so an
agent does not need an untracked concatenation script between import and
workspace initialization.

Save a schema-1 recipe whose source selects the imported shards:

```json
{
  "namespace": "datajig",
  "kind": "prepare",
  "schema_version": 1,
  "source": {
    "format": "csv",
    "include": ["data/*.csv"],
    "ignore": ["data/test*"]
  },
  "output": {"format": "jsonl"},
  "id_field": "id",
  "steps": []
}
```

Then bind the import identity, selected shard bytes, recipe, and predicted
output into one accepted plan:

```bash
datajig prepare-plan raw/demo1/datajig.hf-import.json \
  --recipe recipes/demo1.prepare.json \
  --output data/demo1.jsonl \
  --plan artifacts/demo1.prepare.plan.json

datajig prepare-apply artifacts/demo1.prepare.plan.json \
  --accept-plan prep_...

datajig init data/demo1.jsonl --id-field id
```

The sections below document the identity chain, failure boundaries, and current
product scope directly in the public project overview.

## Prepare tabular data

Turn one CSV, flat Parquet, or JSONL file—or ordered same-format shards from a
recursive local directory or verified import receipt—into deterministic keyed
JSONL with an ordered recipe:

```json
{
  "namespace": "datajig",
  "kind": "prepare",
  "schema_version": 1,
  "source": {"format": "csv"},
  "output": {"format": "jsonl"},
  "id_field": "paper_id",
  "steps": [
    {"op": "trim", "fields": ["paper", "score"]},
    {"op": "replace", "field": "score", "from": "N/A", "to": null},
    {"op": "fill_missing", "field": "score", "value": "0"},
    {"op": "filter", "field": "status", "predicate": "eq", "value": "active"},
    {"op": "rename", "from": "paper", "to": "title"},
    {"op": "cast", "field": "score", "type": "number"},
    {"op": "select", "fields": ["paper_id", "title", "score"]},
    {"op": "dedupe", "by": ["paper_id"], "keep": "first"}
  ]
}
```

For Parquet, use the same recipe and operations with a typed source:

```json
"source": {"format": "parquet"}
```

For JSONL objects, select the JSONL source format:

```json
"source": {"format": "jsonl"}
```

Parquet preparation streams compressed files in the Rust core. It accepts flat
null, boolean, integer, finite floating-point, UTF-8, decimal, date, and
millisecond/microsecond time and timestamp columns. Binary, nested/list/map, and
nanosecond time columns fail closed instead of being silently coerced. A 256 MiB
uncompressed row-group budget rejects compressed inputs that could cause unsafe
decode spikes.

Plan first, then authorize the exact predicted result:

```bash
datajig prepare-plan data/papers.csv \
  --recipe recipes/papers.prepare.json \
  --output data/papers.jsonl \
  --plan artifacts/papers.prepare.plan.json

datajig prepare-apply artifacts/papers.prepare.plan.json \
  --accept-plan prep_...
```

For a recursive local dataset, point the same command at the directory and use
bounded recipe selectors such as `"include": ["year=*/month=*/*.parquet"]` and
`"ignore": ["**/_temporary/**"]`. Selected files are ordered by canonical
relative path; membership changes invalidate apply.

The plan binds direct source bytes, a deterministic `source_set_...`, or the
immutable `hfimport_...` receipt, plus selected shard paths and bytes, recipe,
canonical paths, output row counts, and the predicted JSONL hash. Shards run in
canonical path order with global row, dedupe, and ID state. Apply re-executes the
recipe and publishes only if every identity still matches. Outputs and
provenance receipts use crash-recoverable transaction semantics, never
overwrite existing files, and are safe to retry.
Recipe v1 supports ordered `filter`, `select`, `rename`, `cast`, `trim`, `case`,
`replace`, `fill_missing`, `drop_missing`, and `dedupe` steps. Missing means
JSON `null` or an empty string; trim first when whitespace-only cells should be
treated as missing. `drop_missing` supports `any` and `all` modes.

## Run a bounded SQL transform

Use DuckDB for computation while DataJig remains authoritative for SQL
authorization, input snapshots, limits, identity, canonical JSONL, publication,
and provenance. Install the optional provider, then write one SELECT query:

```sql
SELECT paper_id, title, score
FROM papers
WHERE score >= ?
ORDER BY paper_id
```

Store the scalar parameter array in `params.json`:

```json
[0.8]
```

For one-off calls, pass the same array inline with `--params '[0.8]'`.
Use `--params-file` only when the JSON lives in a file; the two options are
mutually exclusive.

Plan against a local CSV without publishing the output:

```bash
datajig transform-plan \
  --input papers=data/papers.csv \
  --sql-file transforms/high-score.sql \
  --params-file transforms/params.json \
  --id-field paper_id \
  --output data/high-score.jsonl \
  --plan artifacts/high-score.transform.json
```

For Parquet, change only the explicit input path:

```bash
datajig transform-plan \
  --input papers=data/papers.parquet \
  --sql-file transforms/high-score.sql \
  --params-file transforms/params.json \
  --id-field paper_id \
  --output data/high-score.jsonl \
  --plan artifacts/high-score.transform.json
```

Review the bounded preview, then authorize the exact `xform_...` identity:

```bash
datajig transform-apply artifacts/high-score.transform.json \
  --accept-plan xform_...

datajig transform-info \
  data/high-score.jsonl.datajig.transform.json --verify

datajig init data/high-score.jsonl \
  --id-field paper_id \
  --source-receipt data/high-score.jsonl.datajig.transform.json
```

The resulting revision binds the transform plan, provider and DuckDB versions,
source aliases and content IDs, exact SQL and scalar parameters, canonical
output identity, and verified receipt. Planning and apply both execute in a
private staging area. Apply rechecks every source and plan field, reruns the
query, verifies the predicted bytes, and publishes the output and receipt with
recoverable no-clobber transactions.

The Rust core parses SQL into an AST before DuckDB sees it. The v1 policy allows
one `SELECT` over declared aliases and scalar parameters. It rejects table
functions and arbitrary file access, `COPY`, `ATTACH`, extension loading,
network access, multiple statements, unknown or volatile functions, and
undeclared relations. Multi-row output must end its top-level `ORDER BY` with
the ID field. Output fields support nullable values with boolean, signed and
unsigned integer, finite double, or UTF-8 string types; IDs must be non-null
and unique.

Protocol v1 allows at most 16 inputs, 2,000,000 source and output rows, 512 MiB
of source and output bytes, 256 output fields and parameters, 64 KiB of SQL and
parameter JSON, 512 MiB of DuckDB memory, and 15 minutes of wall time. Query the
installed values with `datajig capabilities`; agents should not hard-code them.

Failures are bounded and leave no public partial output. Missing provider errors
include `pip install 'datajig[duckdb]'`; stale sources return transform drift;
an authorization mismatch rejects the apply; an exact retry recovers or returns
the existing verified result. `transform-info --verify` never executes SQL and
can validate a published receipt even when DuckDB is not installed.

## Protect an agent edit

Initialize a keyed JSONL file, then declare why the agent is changing it:

```bash
datajig init data/papers.jsonl --id-field paper_id

datajig changeset-begin \
  --intent "Normalize paper metadata" \
  --task-id research-42
```

After the agent edits the file, freeze and review the exact candidate:

```bash
datajig changeset-stage --change chg_...
datajig check
```

When exactly one declaration and staged candidate match the current dataset,
adapter, and HEAD, `check`, `plan`, `status`, and `seal` resolve that context
automatically. DataJig refuses to guess when the workspace is empty or
ambiguous. Explicit IDs and aliases remain available when choosing among
multiple candidates:

```bash
datajig check --change @active --changeset @latest
```

Workspace workflow commands return JSON with a `decision` and populated
`next_actions`. Follow those actions to inspect findings, restage a fix, or seal
the accepted revision. A failed check immediately returns both the deterministic
remediation-plan command and a bounded findings query as argv arrays; it never
guesses a domain value or silently applies a patch:

```bash
datajig review-plan
datajig findings .datajig/latest.review.json --offset 0 --limit 50

datajig seal \
  --accept-report review_... \
  --message "Accept normalized metadata"
```

Before sealing, DataJig rechecks the live dataset, staged candidate, task anchor,
and review identity. If any of them changed, the operation is rejected.

Every new keyed-JSONL revision also retains its exact original bytes. Recover a
reachable historical revision without changing the tracked dataset or HEAD:

```bash
datajig log
datajig materialize rev_... --output recovered/papers.jsonl
```

Materialization rehashes the stored blob, creates the output atomically, and
never replaces differing content. An exact retry succeeds idempotently. Legacy
revisions created before byte retention remain visible but cannot be invented
from metadata, so DataJig fails explicitly when their content is unavailable.

## Add a quality gate

Pin a JSON policy when the workspace is created:

```json
{
  "namespace": "datajig",
  "schema_version": 1,
  "adapter": "jsonl",
  "mode": "changed_only",
  "fields": {
    "status": {
      "required": true,
      "types": ["string"],
      "enum": ["draft", "published"]
    },
    "score": {"types": ["number"], "minimum": 0, "maximum": 1},
    "doi": {"types": ["string"], "pattern": "^10\\.", "unique": true}
  }
}
```

```bash
datajig init data/papers.jsonl \
  --id-field paper_id \
  --policy data/papers.policy.json
```

`changed_only` blocks new violations introduced by added or modified records.
`full` requires the entire candidate to pass. Policies support required fields,
nullability, JSON types, typed enums, numeric ranges, regex patterns, and scalar
uniqueness.

## Preview, apply, and undo a repair

When a quality check fails, an agent can turn a finding directly into a guarded
scalar repair. DataJig fills in the report, candidate, pseudonymous record, and
record-hash bindings from fresh evidence:

```bash
datajig patch-draft .datajig/latest.review.json fnd_... \
  --change chg_... \
  --changeset changeset_... \
  --after-json 0.9 \
  --output repair.json

datajig patch-preview repair.json .datajig/latest.review.json \
  --change chg_... \
  --changeset changeset_...

datajig patch-apply repair.json .datajig/latest.review.json \
  --change chg_... \
  --changeset changeset_... \
  --accept-patch patch_...
```

Use `--remove` instead of `--after-json` to remove an optional invalid field.
If a finding covers multiple records, first run `locate` and pass one sampled
`rid_...` with `--record`. Both commands return the next fully bound action.

DataJig rechecks the live source and returns a deterministic `patch_...` ID plus
the predicted record hash. `patch-apply` requires that exact ID as explicit
consent, atomically rewrites only the intended physical line, and prepares the
replacement changeset. It never prints the raw record ID, before value, or after
value. Follow the returned `next_actions` to check the new candidate.

Every successful apply returns an opaque `undo_...` handle. Until another edit
or HEAD transition changes its anchors, the exact original line can be restored:

```bash
datajig patch-undo undo_...
```

The one-line preimage is stored under `.datajig/private` with private
permissions; immutable apply and undo receipts contain identities only. Apply and undo are
retry-safe and recover interrupted transactions. A workspace lock coordinates
DataJig writers, while an exact pre-commit fingerprint rejects edits from other
processes. As on any portable filesystem, unrelated software that ignores the
lock cannot be given a universal compare-and-swap guarantee.

## Export verified training data

Export a clean, sealed JSONL revision into deterministic splits and shards:

```bash
datajig export \
  --output artifacts/papers-v1 \
  --seed research-42 \
  --split train=9 \
  --split validation=1

datajig export-info artifacts/papers-v1/datajig.bundle.json --verify
```

Split weights are positive relative integers: repeat `--split NAME=WEIGHT` and
DataJig deterministically normalizes the full set to 10,000 allocation units.
This contract and a complete example are visible in `export --help` and
`capabilities`.
Invalid arguments return structured remediation and executable `next_actions`
instead of requiring an agent to infer the syntax by trial and error.

`--max-shard-records` accepts values from `1`; the final shard may contain fewer
records. `--max-shard-bytes` is a soft target from `1` byte: when one valid
record is larger than the target, that record occupies a shard by itself. The
independent 16 MiB JSONL line safety limit is unchanged.

Reproduce a training bundle from any reachable retained revision—even when the
working file has changed, moved, or been deleted:

```bash
datajig log --state .datajig
datajig export \
  --state .datajig \
  --revision rev_... \
  --output artifacts/papers-rev \
  --split train=9 \
  --split validation=1
```

DataJig verifies the immutable stored bytes before evaluating a view or writing
shards. It never substitutes the current working file for a requested revision.
Older revisions created before content retention fail explicitly instead of
silently exporting different data.

The bundle manifest binds the dataset, sealed revision, record state, split
recipe, shard sizes, and BLAKE3 hashes into one `bundle_...` identity. The output
is standard JSONL, so training code does not need a DataJig runtime.

Bundle publication never overwrites an existing target. If the destination
filesystem cannot provide atomic no-replace directory publication, DataJig
fails before exposing a partial bundle and recommends exporting on a compatible
local filesystem (for example `/tmp`) before moving the verified result.

You can also pin a reproducible cohort before spending training compute:

```json
{
  "namespace": "datajig",
  "kind": "subset_view",
  "schema_version": 1,
  "where": [{"field": "status", "op": "eq", "value": "published"}]
}
```

```bash
datajig view-check --recipe recipes/published.json
datajig export \
  --view recipes/published.json \
  --output artifacts/published-v1 \
  --split train=9 \
  --split validation=1
```

## Load verified training records

Open a bundle with identities supplied by your experiment or CI policy. Rust
verifies the manifest, every shard, record identity, split assignment, ordering,
and optional subset view before Python receives a consumer plan:

```python
from datajig import open_bundle

bundle = open_bundle(
    "artifacts/papers-v1/datajig.bundle.json",
    expected_bundle_id="bundle_...",
    expected_revision_id="rev_...",
    require_assurance="quality_policy",
)

for record in bundle.iter_records("train"):
    train(record)
```

Each shard is rehashed into a private spool before its first record is yielded,
closing the gap between verification and consumption. Optional adapters add no
implicit shuffle, transform, or cache:

```python
from datajig.integrations.torch import iterable_dataset

dataset = iterable_dataset(bundle, split="train")
# torch.utils.data.DataLoader(dataset, batch_size=32)
```

The Hugging Face equivalent is
`datajig.integrations.huggingface.iterable_dataset`. Install `torch` or
`datasets` separately only when using those adapters.

## Prove what crossed the training boundary

When a run needs durable lineage, create a plan for one exact bundle split and
name the external training run:

```bash
datajig consume-plan artifacts/papers-v1/datajig.bundle.json \
  --split train \
  --consumer pytorch \
  --run-id research-42-run-001 \
  --output runs/research-42-run-001 \
  --plan artifacts/research-42-run-001.consume.json
```

Review the response, then pass its exact `consume_...` ID to training:

```python
from datajig import open_consumption
from datajig.integrations.torch import iterable_dataset

run = open_consumption(
    "artifacts/research-42-run-001.consume.json",
    accept_plan="consume_...",
)
dataset = iterable_dataset(run)
```

Plans naming `python` use `run.iter_records()` directly; plans naming PyTorch or
Hugging Face must use that adapter so the recorded boundary cannot be bypassed.
The last worker to exhaust the verified split atomically publishes
`datajig.consumed.json`. Its `consumed_...` identity proves that every verified
record crossed the named DataJig adapter boundary at least once. Early stop,
exceptions, changed shards, or forged run state produce no receipt. This is not
proof of training success, gradient use, post-adapter ordering, or exactly-once
delivery.

## ImageFolder review

For image classification datasets, DataJig can identify:

- added, removed, renamed, relabeled, and split-moved samples;
- exact and perceptual duplicates across train/validation/test;
- corrupt or unsupported media;
- label, split, format, size, aspect-ratio, and channel drift.

The human-facing `compare` command produces HTML and JSON. Native `inventory` and
`review` commands expose the same workflow as bounded agent-facing artifacts.

## Built for agents

```bash
datajig capabilities
datajig describe
datajig describe check
datajig artifact-schema
datajig artifact-schema prepare-recipe
datajig artifact-schema subset-view
datajig artifact-schema training-consumption-plan
datajig artifact-schema training-consumption-receipt
datajig agent-skill --output .agents/skills/datajig/SKILL.md
```

- **Discoverable:** versioned command and artifact contracts expose inputs,
  effects, limits, schemas, canonical examples, platforms, outputs, and exits.
- **Bounded:** summaries and finding pages have hard limits; full evidence stays
  in artifacts.
- **Addressable:** stable `chg_...`, `changeset_...`, `fnd_...`, `review_...`,
  `hfplan_...`, `hfimport_...`, `prep_...`, `patch_...`, `apply_...`,
  `undo_...`, `revert_...`, `rev_...`, `view_...`, `bundle_...`, `consume_...`,
  and `consumed_...` IDs connect every step.
- **Recoverable:** commands return explicit decisions and next actions instead of
  relying on prose or hidden state.
- **Race-aware:** live bytes are revalidated before review evidence can advance
  the workspace HEAD.

## Supported today

| Workflow | Status |
| --- | --- |
| Revision-pinned Hugging Face dataset-repository import | Available on Linux and macOS |
| Keyed JSONL inspection and record diff | Available |
| Privacy-safe CSV/Parquet inspection and recipe scaffold | Available |
| Recursive CSV/Parquet/JSONL dataset preparation with membership identity | Available on Linux and macOS |
| Deterministic CSV/flat Parquet cleaning with plan/apply provenance | Available on Linux and macOS |
| Task-scoped JSONL workspace and quality policy | Available on Linux and macOS |
| Evidence-bound JSONL patch draft, preview, atomic apply, and exact undo | Available on Linux and macOS |
| Sealed subset views and verified training export | Available on Linux and macOS |
| Verified Python, PyTorch, and Hugging Face consumption | Available |
| ImageFolder semantic review and HTML report | Available |
| Universal directory snapshots | Available |
| Runnable end-to-end tutorial | Available on Linux and macOS |
| Linux x86_64/aarch64 and macOS x86_64/arm64 wheels | Published |
| Windows through WSL | Supported using the Linux wheel |
| Native Windows workspace writes | Not yet supported |
| S3/GCS/Azure, Oxen, and DVC source adapters | Planned |
| Automatic model-training orchestration | Out of scope |

DataJig is experimental. Artifact schemas and CLI contracts are versioned, but
the project has not reached a stable `1.0` compatibility promise.

## Python API

Python exposes human-facing ImageFolder comparison and verified training-bundle
consumption:

```python
from datajig import compare, open_bundle

report = compare("baseline/", "candidate/", workers=4)
print(report.policy.status)

bundle = open_bundle(
    "artifacts/papers-v1/datajig.bundle.json",
    expected_bundle_id="bundle_...",
    expected_revision_id="rev_...",
)
print(bundle.bundle_id, bundle.splits)
```

Without an expected bundle or revision ID, `open_bundle` still checks internal
integrity but does not establish that the bundle is the one your experiment or
CI policy intended. Pin at least one trusted identity for training inputs.

The authoritative agent workspace, identity, review, and export paths run in
Rust. Python is the packaging and integration layer, not an algorithm fallback.

## Develop from source

```bash
git clone https://github.com/liukejun7/DataJig.git
cd DataJig
cargo build --locked --manifest-path rust/Cargo.toml
python -m pip install -e '.[dev]'
export DATAJIG_NATIVE="$PWD/rust/target/debug/datajig-core"
datajig capabilities
```

Requirements: Python 3.11+ and Rust 1.85+.

## Current direction

DataJig is becoming the default control layer between an agent and its data:

1. Arrow batch execution, joins, dataset-wide numeric transforms, and multi-record patch sets;
2. XLSX and database snapshot adapters, followed by remote Spark/Hive catalog manifests;
3. revision adapters for S3/GCS/Azure, Oxen, DVC, and additional dataset hubs;
4. native Windows filesystem semantics and wheels.

The goal is simple: an agent should always know what it changed, prove what it
reviewed, recover safely, and hand training code a verified input.
