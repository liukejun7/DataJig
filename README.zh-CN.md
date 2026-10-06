# DataJig

[English](README.md) · [架构](ARCHITECTURE.md) · [路线图](ROADMAP.md) · [贡献指南](CONTRIBUTING.md) · [安全策略](SECURITY.md) · [更新日志](CHANGELOG.md)

<p align="center">
  <img src="assets/datajig-hero.png" alt="DataJig 将数据集修改对齐为可验证 revision" width="100%">
</p>

<p align="center">
  <strong>从 raw dataset 到可验证训练输入的 Agent 原生数据供应链。</strong><br>
  导入、处理、审查、版本化、导出与校验，全程不丢失数据身份。
</p>

DataJig 把上游原始 bytes 和 Agent 的一次数据修改变成可追溯训练数据事务：

```text
锁定源 → 导入 receipt → 确定性处理 → 审查 → 接受 revision → 导出
            │                    ↑                         │
            └── content ID ──────┴──────────── 验证后消费 ┘
```

它不是另一个 Git、DVC 或 Oxen。DataJig 位于存储与处理工具之上，控制 Agent
真正修改数据的那一刻，并向训练代码交付已锁定身份、已校验完整性的记录。
任务、候选、审查、接受的 revision、数据子集和训练 bundle 都有确定性身份；
证据过期或混入计划外修改时会拒绝继续。

> 版本 `0.5.1` · 本地优先 · Rust 原生内核 · 支持 CSV、Parquet、JSONL 与 ImageFolder

## 为什么需要 DataJig

Agent 很会改数据，但一次脚本成功并不能回答：

- Agent 审查的究竟是哪一份数据？
- 审查之后文件是否又被其他进程修改？
- 修改是否属于当前声明的任务？
- 为什么这个版本被接受？
- 训练最终读到了哪些记录和 shard bytes？

DataJig 用一套小而确定的机器协议回答这些问题。完整证据保存在本地文件中，
stdout 只返回适合 Agent 上下文窗口的有界 JSON。

## 快速开始

安装稳定版。PyPI wheel 已经内置 Rust 内核：

```bash
python -m pip install datajig
datajig capabilities
```

无需准备输入文件，先用一条命令实际跑完整流程：

```bash
datajig tutorial ./datajig-tutorial
```

命令要求目标目录尚不存在，并自动生成一个小型 keyed JSONL 数据集、不可变 workspace、
一次经过审查和 seal 的 Agent 修改，以及可验证训练 bundle。有界 JSON 响应会返回所有
关键身份和精确的 `export-info` 校验动作。

发现类命令会返回确定性的 `agent_contract_id`，覆盖命令目录和输入 artifact schema。
当 Agent 或 CI 需要在协议未经审查即发生变化时 fail closed，可以锁定这个身份；生成的
Agent Skill 也会记录同一个身份。

用一条命令即可把这套协议安装进任意 Git worktree：

```bash
datajig repository-install --root .
datajig repository-check --root .
```

安装器会生成版本匹配的 Agent Skill、固定 action commit 的 GitHub Actions workflow、
可执行的 pre-commit hook，并最后写入作为提交点的内容寻址
`.datajig-repository.json`。Lock 精确绑定 DataJig 版本、Agent 合同、受管文件 bytes、
启用组件和仓库相对 workspace。安装可幂等重试并能从中断中恢复；遇到未知或本地修改
的目标、冲突的 `core.hooksPath`、符号链接、硬链接，以及会静默删除组件或 state 绑定
的升级时会拒绝覆盖。

`repository-check` 完全只读：协议或文件发生漂移就会失败，并验证每个绑定 workspace
仍对应干净的数据集 HEAD。全新 CI clone 中可用 `--ci`，它只跳过 clone-local 的
`core.hooksPath` 检查，其余内容与 workspace 检查保持不变。首次安装时可用
`--no-hook` 或 `--no-github-actions` 省略可选组件。只有当 `.datajig` 等 workspace
目录会在所有执行检查的环境（包括 CI）中被显式还原时，才添加可重复的
`--state .datajig` 绑定；DataJig 不会隐式上传或恢复被忽略的 workspace state。

对 JSONL、CSV 或 flat Parquet，都从同一个隐私安全命令开始：

```bash
datajig inspect data/papers.jsonl --id-field paper_id
datajig inspect data/papers.csv --id-field paper_id
datajig inspect data/papers.parquet --id-field paper_id
```

`inspect` 会报告 schema、行数、缺失值计数、稳定的源身份和 ID 健康度，但不会打印
cell value 或原始 ID。对于 CSV 和 Parquet，它还会返回可编辑的 preparation recipe
模板和 `prepare-plan` 下一步；对于 JSONL，文件就绪时会建议初始化 workspace。

CSV 和 flat Parquet 已是原生 inspect 与确定性 prepare 输入。任务级事务 workspace
当前以 keyed JSONL 为核心，因此表格数据会先准备为 JSONL 再进入变更控制。XLSX 与
数据库、Hive、Spark 直连 adapter 尚未实现。

无需创建 workspace，也能比较两个 keyed JSONL revision：

```bash
datajig record-diff baseline/papers.jsonl data/papers.jsonl \
  --id-field paper_id
```

`record-diff` 按稳定 ID 对齐记录，区分新增、删除、修改和移动。

对于 ImageFolder 数据集，可以直接生成自包含报告：

```bash
datajig compare baseline/ candidate/ \
  --output review.html \
  --json review.json
```

## 导入不可变的 Hugging Face 数据集

先把数据集 branch 或 tag 解析一次，审查选中的仓库文件，再授权执行这份精确计划：

```bash
mkdir -p raw artifacts

datajig hf-import-plan lhoestq/demo1 \
  --revision main \
  --include 'data/*.csv' \
  --output raw/demo1 \
  --plan artifacts/demo1.hf-plan.json

# 审查计划后，从有界 JSON 响应中复制 plan_id。
datajig hf-import-apply artifacts/demo1.hf-plan.json \
  --accept-plan hfplan_...

datajig artifact-schema prepare-recipe
```

规划阶段把 `main` 解析成 40 位 commit，并记录精确文件集合与大小。应用阶段只下载
该 commit，逐文件校验，并把本地 BLAKE3 身份写入 `datajig.hf-import.json`，随后
原子发布目录。已有输出只有在 receipt 与每个文件仍完全匹配时才被接受，因此重试
可以保持幂等，也不会覆盖数据。

公开、私有与 gated 仓库沿用 Hugging Face 标准 token 和 endpoint 配置。首版 adapter
导入 dataset repository 中的文件，不解释 Dataset Viewer 的 configuration 或生成 split。
导入 receipt 可以直接交给 `prepare-plan`，Agent 无需在导入和 workspace 之间另写一段
无法追溯的拼接脚本。Recipe 可用 `include` / `ignore` 选择同格式 shard：

```json
"source": {"format": "csv", "include": ["data/*.csv"], "ignore": ["data/test*"]}
```

```bash
datajig prepare-plan raw/demo1/datajig.hf-import.json \
  --recipe recipes/demo1.prepare.json \
  --output data/demo1.jsonl \
  --plan artifacts/demo1.prepare.plan.json

datajig prepare-apply artifacts/demo1.prepare.plan.json --accept-plan prep_...
datajig init data/demo1.jsonl --id-field id
```

## 准备表格数据

用一份有序 recipe，把单个 CSV、flat Parquet、JSONL，或 verified import receipt 中
按路径排序的同格式 shards，转换成确定、带稳定主键的 JSONL：

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

Parquet 使用同一份 recipe 与操作，只需把 source 改为类型化输入：

```json
"source": {"format": "parquet"}
```

JSONL object 输入使用：

```json
"source": {"format": "jsonl"}
```

Parquet preparation 由 Rust 内核流式读取压缩文件，支持 flat null、布尔、整数、有限
浮点、UTF-8、decimal、日期，以及毫秒/微秒精度的时间和 timestamp 列。Binary、
nested/list/map 与纳秒时间列会明确失败，不会被静默强制转换。256 MiB 的未压缩
row-group 预算会在解码前拒绝可能造成危险内存峰值的压缩输入。

先计划，再授权精确的预测结果：

```bash
datajig prepare-plan data/papers.csv \
  --recipe recipes/papers.prepare.json \
  --output data/papers.jsonl \
  --plan artifacts/papers.prepare.plan.json

datajig prepare-apply artifacts/papers.prepare.plan.json \
  --accept-plan prep_...
```

Plan 会绑定源文件 bytes 或 `hfimport_...` receipt、选中 shard 的路径与 bytes、recipe、
规范路径、输出行数和预测 JSONL 哈希。所有 shards 共享全局行数、去重与 ID 状态。Apply
重新执行 recipe，只有全部身份仍一致时才发布。数据与 provenance receipt 使用
可从崩溃恢复的事务语义、不覆盖已有文件，并且可以安全重试。Recipe v1 支持有序的
`filter`、`select`、`rename`、`cast`、`trim`、`case`、`replace`、
`fill_missing`、`drop_missing` 和 `dedupe`。缺失值指 JSON `null` 或空字符串；若要
把纯空白单元格视为缺失值，应先执行 `trim`。`drop_missing` 支持 `any` 与 `all` 模式。

## 保护一次 Agent 修改

先初始化 keyed JSONL，再声明 Agent 为什么要修改它：

```bash
datajig init data/papers.jsonl --id-field paper_id

datajig changeset-begin \
  --intent "Normalize paper metadata" \
  --task-id research-42
```

Agent 修改文件后，冻结并审查精确候选：

```bash
datajig changeset-stage --change chg_...
datajig check
```

当且仅当当前 dataset、adapter 与 HEAD 对应唯一 declaration 和 staged candidate 时，
`check`、`plan`、`status`、`seal` 会自动解析上下文；没有候选或存在歧义时拒绝猜测。
需要从多个候选中选择时仍可显式使用完整 ID 或别名：

```bash
datajig check --change @active --changeset @latest
```

Workspace 工作流命令会返回带 `decision` 和完整参数的 `next_actions`。Agent
可以按返回动作继续查看 finding、修复并重新 stage，或接受当前 revision。检查失败
时会直接返回确定性 remediation plan 和有界 findings 查询的 argv 数组；不会猜测
业务值，也不会静默应用 patch：

```bash
datajig plan
datajig findings .datajig/latest.review.json --offset 0 --limit 50

datajig seal \
  --accept-report review_... \
  --message "Accept normalized metadata"
```

在 seal 之前，DataJig 会重新验证现场数据、staged candidate、任务锚点和审查
身份。任何一项发生变化，操作都会被拒绝。

每个新 keyed-JSONL revision 还会保留精确的原始 bytes。无需改变 tracked dataset
或 HEAD，即可恢复任意可达历史版本：

```bash
datajig log
datajig materialize rev_... --output recovered/papers.jsonl
```

Materialize 会重新哈希存储的 blob，以原子方式创建输出，且绝不覆盖不同内容；
完全相同的重试会幂等成功。启用 byte retention 之前创建的旧 revision 仍可查看，
但 DataJig 不会根据 metadata 猜造历史内容，缺失时会明确失败。

## 增加质量门槛

初始化 workspace 时固定 JSON 策略：

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

`changed_only` 只阻止新增或修改记录引入的新违规；`full` 要求整个候选满足
策略。当前支持 required、nullability、JSON 类型、typed enum、数值范围、
正则表达式和标量唯一性。

## 预览、应用与撤销一次修复

质量检查失败后，Agent 可以直接把 finding 变成一次受保护的标量修复。DataJig
会从新鲜证据中补齐 report、candidate、匿名 record 和记录哈希绑定：

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

删除可选的非法字段时，把 `--after-json` 换成 `--remove`。如果 finding 覆盖多条
记录，先运行 `locate`，再用 `--record` 传入一条 sampled `rid_...`。两条命令都会
返回下一步所需的完整绑定。

DataJig 会重新检查现场数据，并返回确定性的 `patch_...` ID 和预测记录哈希；
`patch-apply` 必须显式接受这个精确 ID，才会原子替换目标物理行并准备新的
changeset。它不会打印原始 record ID、修改前值或修改后值。Agent 按返回的
`next_actions` 复查新候选即可。

每次成功应用都会返回一个不透明的 `undo_...` 句柄。在其他修改或 HEAD 迁移改变
事务锚点前，可以精确恢复原始行：

```bash
datajig patch-undo undo_...
```

单行 preimage 只保存在 `.datajig/private`，并使用私有权限；不可变的 apply/undo
receipt 仅含身份。
apply 与 undo 可安全重试，也能恢复中断事务。workspace lock 协调 DataJig 写入者，
提交前的精确 fingerprint 会拒绝其他进程造成的修改。与所有可移植文件系统方案
一样，无法对完全无视锁的外部软件承诺通用 compare-and-swap。

## 导出可验证训练数据

把 clean、sealed JSONL revision 导出为确定性 split 和 shard：

```bash
datajig export \
  --output artifacts/papers-v1 \
  --seed research-42 \
  --split train=9000 \
  --split validation=1000

datajig export-info artifacts/papers-v1/datajig.bundle.json --verify
```

Split 权重使用整数万分比：重复传入 `--split NAME=WEIGHT`，每个权重必须为正，
且总和必须恰好等于 `10000`。`export --help` 和 `capabilities` 都会直接暴露这一
契约与完整示例。参数不合法时会返回结构化修复建议和可执行的 `next_actions`，
Agent 不需要靠反复试错推断语法。

`--max-shard-records` 可从 `1` 开始设置，最后一个 shard 可以少于目标记录数。
`--max-shard-bytes` 是从 `1` byte 开始的软目标：若一条合法记录本身超过目标，它会
独占一个 shard。独立的 16 MiB JSONL 单行安全上限保持不变。

任意可达且保留了内容的历史 revision 都能直接重建训练包；即使工作文件已被
修改、移动或删除也不受影响：

```bash
datajig log --state .datajig
datajig export \
  --state .datajig \
  --revision rev_... \
  --output artifacts/papers-rev \
  --split train=9000 \
  --split validation=1000
```

DataJig 会先验证不可变历史内容，再执行 view 和生成 shard；绝不会拿当前工作
文件冒充指定 revision。未保留内容的早期 revision 会明确失败，不会静默导出错误数据。

bundle manifest 将 dataset、sealed revision、record state、split recipe、shard
大小与 BLAKE3 哈希绑定为一个 `bundle_...` 身份。输出仍是标准 JSONL，训练代码
不需要依赖 DataJig runtime。

Bundle 发布绝不会覆盖已有目标。如果目标文件系统不支持目录级原子 no-replace
发布，DataJig 会在暴露任何不完整 bundle 前失败，并建议先在兼容的本地文件系统
（例如 `/tmp`）导出，再移动已经验证的结果。

也可以在花训练算力之前先固定可复现 cohort：

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
  --split train=9000 \
  --split validation=1000
```

## 在训练中读取验证过的记录

用实验或 CI 策略提供的身份打开 bundle。Rust 会先验证 manifest、全部 shard、
记录身份、split 分配、确定性顺序和可选 subset view，再把消费计划交给 Python：

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

每个 shard 都会先重新哈希到私有 spool，确认没有在验证后被替换，才会交出第一条
记录。可选 adapter 不会暗中 shuffle、transform 或缓存：

```python
from datajig.integrations.torch import iterable_dataset

dataset = iterable_dataset(bundle, split="train")
# torch.utils.data.DataLoader(dataset, batch_size=32)
```

Hugging Face 对应入口是
`datajig.integrations.huggingface.iterable_dataset`。只有实际使用时才需要另外安装
`torch` 或 `datasets`。

## 证明哪些数据越过了训练边界

需要持久训练血缘时，为一个精确 bundle split 创建计划，并绑定外部训练 run：

```bash
datajig consume-plan artifacts/papers-v1/datajig.bundle.json \
  --split train \
  --consumer pytorch \
  --run-id research-42-run-001 \
  --output runs/research-42-run-001 \
  --plan artifacts/research-42-run-001.consume.json
```

审查响应后，把返回的精确 `consume_...` ID 交给训练代码：

```python
from datajig import open_consumption
from datajig.integrations.torch import iterable_dataset

run = open_consumption(
    "artifacts/research-42-run-001.consume.json",
    accept_plan="consume_...",
)
dataset = iterable_dataset(run)
```

指定 `python` 的计划直接使用 `run.iter_records()`；指定 PyTorch 或 Hugging Face
的计划必须通过对应 adapter，避免绕过 receipt 所声明的边界。
最后一个耗尽已验证 split 的 worker 会原子发布 `datajig.consumed.json`。
其中的 `consumed_...` 身份证明：每条已验证记录至少一次越过了指定的 DataJig
adapter 边界。提前停止、异常、shard 改变或伪造运行状态都不会产生 receipt。
它不证明训练成功、梯度更新、adapter 之后的顺序或 exactly-once 投递。

## ImageFolder 审查

对于图像分类数据集，DataJig 可以识别：

- 新增、删除、重命名、标签变化和 split 移动；
- train/validation/test 之间的完全重复与视觉近似重复；
- 损坏或不支持的媒体；
- 标签、split、格式、尺寸、宽高比和通道分布漂移。

人工使用的 `compare` 命令生成 HTML 与 JSON；原生 `inventory` 和 `review`
命令则提供适合 Agent 的有界 artifact。

## 为 Agent 设计

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

- **可发现：** 版本化命令与 artifact 契约公开输入、影响、限制、schema、标准示例、
  平台、输出和退出码。
- **有边界：** 摘要与 finding 分页有硬上限，完整证据留在 artifact 中。
- **可引用：** `chg_...`、`changeset_...`、`fnd_...`、`review_...`、
  `hfplan_...`、`hfimport_...`、`prep_...`、`patch_...`、`apply_...`、
  `undo_...`、`revert_...`、`rev_...`、`view_...`、`bundle_...`、
  `consume_...` 与 `consumed_...` 串联整个任务。
- **可恢复：** 命令返回明确 decision 和 next action，不依赖隐藏状态。
- **防竞态：** 只有重新验证现场 bytes 后，审查证据才能推进 workspace HEAD。

## 当前支持范围

| 工作流 | 状态 |
| --- | --- |
| 锁定 revision 的 Hugging Face dataset repository 导入 | Linux、macOS 已支持 |
| Keyed JSONL inspect 与 record diff | 已支持 |
| 隐私安全的 CSV/Parquet inspect 与 recipe scaffold | 已支持 |
| 带 plan/apply provenance 的确定性 CSV/flat Parquet 清洗 | Linux、macOS 已支持 |
| 任务级 JSONL workspace 与质量策略 | Linux、macOS 已支持 |
| 证据绑定的 JSONL patch 草拟、预览、原子应用与精确撤销 | Linux、macOS 已支持 |
| Sealed subset view 与训练导出 | Linux、macOS 已支持 |
| Python、PyTorch、Hugging Face 验证消费 | 已支持 |
| ImageFolder 语义审查与 HTML 报告 | 已支持 |
| 通用目录 snapshot | 已支持 |
| 可运行的端到端 tutorial | Linux、macOS 已支持 |
| Linux x86_64/aarch64、macOS x86_64/arm64 wheel | 已发布 |
| Windows 通过 WSL 使用 | 使用 Linux wheel 即可 |
| Windows 原生 workspace 写入 | 暂未支持 |
| S3/GCS/Azure、Oxen 与 DVC source adapter | 计划中 |
| 自动训练编排 | 不在产品边界内 |

DataJig 仍处于实验阶段。Artifact schema 和 CLI contract 都有版本号，但项目
尚未作出稳定 `1.0` 兼容承诺。

## Python API

Python API 同时服务人工 ImageFolder 对比和验证过的训练 bundle 消费：

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

如果没有传入期望的 bundle 或 revision ID，`open_bundle` 仍会校验内部完整性，
但不能证明它就是实验或 CI 策略指定的那一份。训练输入至少应锁定一个可信身份。

Agent workspace、身份、审查与训练导出的权威实现位于 Rust。Python 是打包与
生态集成层，不是算法 fallback。

## 从源码开发

```bash
git clone https://github.com/liukejun7/DataJig.git
cd DataJig
cargo build --locked --manifest-path rust/Cargo.toml
python -m pip install -e '.[dev]'
export DATAJIG_NATIVE="$PWD/rust/target/debug/datajig-core"
datajig capabilities
```

环境要求：Python 3.11+、Rust 1.85+。

## 接下来的方向

DataJig 将成为 Agent 与数据之间的默认控制层：

1. Arrow batch 执行、join、全数据集数值变换与多记录 patch set；
2. XLSX 与数据库快照 adapter，随后接入 Spark/Hive manifest；
3. 对接 S3/GCS/Azure、Oxen、DVC 与更多数据集 Hub 的 revision adapter；
4. Windows 原生文件系统语义与 wheel。

目标很简单：Agent 永远知道自己改了什么，能证明审查了什么，能够安全恢复，
并把可验证的输入交给训练代码。
