# Hybrid Storage Lakehouse Trade-off Research Plan

## Working Title

Trade-off Analysis and Optimization of a Hybrid Lakehouse Architecture Using Cloud Object Storage and Metadata Catalogs

## Purpose

This document is the canonical research plan for the project. Future changes to the thesis direction, research questions, architecture scope, benchmark design, and optimization experiments should update this file first.

The research focuses on a hybrid lakehouse architecture where compute remains local while storage and catalog services move to AWS.

## Core Thesis

A hybrid lakehouse can externalize storage durability, metadata management, and cloud integration by moving object storage and catalog services to AWS, but this introduces measurable tradeoffs in performance, cost, reliability, operability, and portability.

The goal is not to prove that hybrid is always better than on-premises. The goal is to measure the tradeoffs and evaluate whether practical file-layout and query-layout optimizations reduce the main penalties of hybrid object storage.

## Research Questions

### RQ1: Hybrid Storage Tradeoffs

What performance, cost, reliability, and operability tradeoffs appear when moving lakehouse storage/catalog from local infrastructure to AWS?

This question covers the baseline comparison between the current local lakehouse and the hybrid S3 + Iceberg + Glue Catalog architecture.

### RQ2: Optimization Impact

Can practical file-layout and query-layout optimizations reduce the observed hybrid-storage penalties?

This question covers targeted experiments after the hybrid baseline is measurable. It does not require perfect tuning; it only tests whether common, practical optimizations improve the observed tradeoffs.

## Hypotheses

### H1: Hybrid Storage Tradeoff

Hybrid storage/catalog improves durability and reduces local storage-management burden, but increases latency, request dependency, IAM/config complexity, and possibly runtime cost.

Mapped to:

- RQ1
- baseline on-premises vs hybrid comparison
- performance, cost, reliability, and operability measurements

### H2: File-layout And Query-layout Optimization

File-size control and partition-aware queries reduce query latency and request overhead in the hybrid architecture.

Mapped to:

- RQ2
- file-size/small-file control experiment
- partition-aware query experiment
- Trino query latency and S3 request-overhead measurements

### H3: Spark Parallelism Limit

Increasing Spark parallelism improves ingestion only up to the point where remote object-store/network overhead dominates.

Mapped to:

- RQ2
- optional Spark executor-sizing experiment
- ingestion runtime, stability, and diminishing-return measurements

## Architecture Scope

### On-Premises Baseline

```text
Local Airflow
  -> local Spark Standalone
  -> local Trino
  -> MinIO-compatible object storage
  -> Apache Iceberg tables
  -> current local Iceberg catalog
```

The on-premises baseline is the control architecture. It represents the current self-managed lakehouse MVP.

### Hybrid Storage Baseline

```text
Local Airflow
  -> local Spark Standalone
  -> local Trino
  -> AWS S3 raw data
  -> AWS S3 Iceberg warehouse
  -> Apache Iceberg v2 tables
  -> AWS Glue Data Catalog
```

The hybrid baseline moves only the storage and catalog layers to AWS. Compute remains local so that the study can isolate the tradeoffs of remote storage and cloud metadata.

### Explicitly Out Of Scope

The following are not part of the main baseline:

- AWS Glue ETL jobs as Spark compute.
- S3 Tables as managed Iceberg table storage.
- EMR, Databricks, Athena-only, or fully cloud-hosted lakehouse variants.
- Migrating Airflow or Spark workers to AWS.

These may be discussed as future work or extension variants after the main comparison is complete.

## Locked Hybrid Baseline Decision

The main hybrid baseline is:

```text
S3 + Apache Iceberg + AWS Glue Data Catalog
```

Not:

```text
S3 Tables
AWS Glue ETL
full cloud migration
```

This keeps attribution clean:

- Storage changes from MinIO to S3.
- Catalog changes from local catalog to Glue Data Catalog.
- Table format remains Apache Iceberg.
- Compute remains local Spark and local Trino.
- Pipeline logic remains unchanged.

## Benchmark Dependency

The reproducible benchmark pipeline must be implemented before hybrid evaluation.

The benchmark pipeline should provide:

- repeatable workload definitions
- environment profiles
- fixed partition/repetition inputs
- Airflow-triggered Spark pipeline runs
- Trino query suite execution
- JSON artifacts
- Iceberg-backed benchmark metrics
- config hash and git SHA capture

The same benchmark harness must be used for both on-premises and hybrid runs.

## Current Execution Status

As of 2026-07-07, the Phase 1 on-premises smoke benchmark has passed for the
local profile.

Accepted smoke run:

```text
benchmark_run_id = bench_onprem_smoke_20260707T142202Z_d79fe1b
config_hash = d857d7fa8913bc05c0fbd56c81e8e72769f630127b3a55bdf7af595ed49e664d
artifact_path = benchmarks/artifacts/bench_onprem_smoke_20260707T142202Z_d79fe1b/
```

Evidence captured:

- 6 successful Airflow DAG runs:
  - 3 repetitions for `yellow 2011-01`
  - 3 repetitions for `green 2014-01`
- 18 successful Spark task metrics with Spark History application IDs.
- 12 Iceberg partition/file-layout metrics.
- 70 successful Trino query metrics.
- 106 total normalized metrics inserted into
  `lakehouse.benchmark.run_metrics`.
- Repeated partition writes preserved stable Silver and Gold row/file metrics.
- Quality summaries were scoped to the current `benchmark_run_id`.
- Storage-sensitive queries processed materially more bytes than metadata-only
  checks.

This result replaces the earlier untrusted smoke evidence as the accepted local
smoke baseline. It is not a full comparative result and must not be used as
evidence for hybrid tradeoffs by itself.

Next phase:

Begin Phase 2 by adding the AWS S3 + Glue profile and infrastructure needed to
run the same benchmark harness against the locked hybrid baseline.

Phase 2 execution plan:

```text
docs/research_plan/phase2_hybrid_storage_baseline_plan.md
```

Phase 2 repo scaffolding added:

- `conf/environments/hybrid_aws.toml`
- `conf/spark/spark-defaults.hybrid-aws.conf`
- `conf/trino/catalog/lakehouse_hybrid.properties`
- `src/etl/sql/hybrid_aws/*.sql`
- `scripts/aws/bootstrap_phase2_hybrid.py`

As of 2026-07-08, the Phase 2 hybrid smoke benchmark has passed for the
`hybrid_aws` profile.

Accepted hybrid smoke run:

```text
benchmark_run_id = bench_hybrid_aws_smoke_20260708T053029Z_d79fe1b
config_hash = 768f8591dbb2841a1ea9c7cdd7b248830a8d8fae45a0b8e5edbdad4b7cd64eb9
artifact_path = benchmarks/artifacts/bench_hybrid_aws_smoke_20260708T053029Z_d79fe1b/
```

Evidence captured:

- 6 successful Airflow DAG runs:
  - 3 repetitions for `yellow 2011-01`
  - 3 repetitions for `green 2014-01`
- 18 successful Spark task metrics.
- 12 Iceberg partition/file-layout metrics.
- 70 successful Trino query metrics.
- 106 total normalized metrics inserted into
  `lakehouse.benchmark.run_metrics`.
- Glue-backed Silver row counts were readable through Trino:
  - `yellow 2011-01`: 13,393,301 rows
  - `green 2014-01`: 803,609 rows

This result is the accepted Phase 2 hybrid smoke baseline. The earlier
green-only smoke run remains useful as setup evidence only and must not be used
as the comparative hybrid benchmark result.

### Phase 3 Execution Status

As of 2026-07-27, the first official Phase 3 attempt is invalidated and the
recovery baseline has passed fresh preparation, full-workload preflight on both
architectures, and isolated AWS evidence initialization.

#### Invalidated baseline

```text
comparison_id = phase3_baseline_20260722T172509Z_59282a4
frozen_commit = 59282a45a67bba1015456a7f2421f194ce431044
state = benchmarks/artifacts/comparisons/phase3_baseline_20260722T172509Z_59282a4/comparison_run.json
failed_pair = pipeline-01
failed_member = onprem
failed_dag_run = phase3_baseline_20260722T172509Z_59282a4__pipeline-01__a01__onprem__green_2014_07__r01
failed_task = check_silver_quality
```

- The attempt passed its identity, credential, clean-commit, and new-target
  gates but failed before any hybrid member or report ran.
- The failed partition contained 1,273,735 rows. Seventeen rows had negative
  `tip_amount` and five had negative `tolls_amount`.
- Those rows were coherent refund or void records with signed fare, taxes,
  payment components, and total. They were not staging corruption.
- The old Pandera schema allowed signed fare and total values but prohibited
  signed tips and tolls. That hard rule was internally inconsistent.
- The old two-partition preflight covered January only and did not exercise the
  failing July partition.
- All partial artifacts, database metrics, resource samples, and the incomplete
  evidence window are preserved. The old identifier is permanently invalid;
  `--resume`, manual DAG retry, or a second invocation with it is prohibited.

#### Accepted recovery baseline

```text
comparison_id = phase3_baseline_20260727T035807Z_fde426a
frozen_commit = fde426a8031a8ea101d470b54a8f0de5d4207336
manifest = benchmarks/artifacts/phase3_preparation/phase3_baseline_20260727T035807Z_fde426a_manifest.json
onprem_preflight = phase3_baseline_20260727T035807Z_fde426a__preflight__onprem
hybrid_preflight = phase3_baseline_20260727T035807Z_fde426a__preflight__hybrid_aws
evidence_snapshot = benchmarks/artifacts/phase3_evidence/phase3_baseline_20260727T035807Z_fde426a/snapshot.json
evidence_captured_at = 2026-07-27T04:43:50.947492+00:00
```

Recovery semantics:

- Signed `tip_amount`, `tolls_amount`, and derived `tip_ratio` values remain in
  both datasets.
- `negative_tip_amount_rows` and `negative_tolls_amount_rows` are soft checks;
  nonzero counts have status `warn` and do not cause process failure.
- Structural, partition, derivation, and Pandera failures remain hard. Audit
  rows are still written before a hard failure exits nonzero.
- Tables use new `phase3_v2_silver`, `phase3_v2_quality`, and
  `phase3_v2_gold` namespaces. Hybrid warehouse data uses
  `warehouse/phase3_v2`, isolating initial table state and storage evidence from
  the invalidated run.
- Raw inputs remain under `phase3/raw`; no partition was filtered or replaced.

Accepted preparation and preflight evidence:

- The manifest contains 16 objects across two architectures with zero local to
  remote SHA-256 mismatches and six verified namespace records.
- Both preflights used the official eight-partition
  `benchmarks/workloads/phase3_comparative.toml` workload.
- Each architecture completed 8/8 DAG runs, 24/24 successful Spark tasks, 24
  unique Spark application IDs, 56/56 finished Trino queries with IDs, and 104
  normalized metrics.
- Each artifact's 104 metrics match 104 rows in
  `lakehouse.benchmark.run_metrics`.
- All 56 query results match across architectures after deterministic row
  sorting.
- Both July quality audits contain hard passes for row count, validity
  derivation, and Pandera validation, plus soft warnings with observed counts
  17 and 5.

Accepted AWS evidence initialization:

- `phase3/raw/`: 8 objects and 805,120,908 bytes.
- `warehouse/phase3_v2/`: 100 objects and 928,063,948 bytes after the hybrid
  preflight.
- `BucketSizeBytes` and `NumberOfObjects` each returned two datapoints for both
  buckets.
- AWS Pricing returned 177 Amazon S3 products for `us-east-1`.
- The invalidated root snapshot remains preserved; current evidence uses the
  comparison-specific path above.

#### Official recovery comparison runbook

1. Preserve and commit this plan update. Do not change any recovery code,
   profiles, workload, harness, or report logic; such a change invalidates the
   accepted commit and requires another preparation and both preflights.
2. Verify the AWS principal and Spark worker credential mounts without printing
   credentials:

   ```bash
   aws sts get-caller-identity --profile lakehouse-aws

   for container in spark-worker-1 spark-worker-2 spark-worker-3; do
     docker exec "$container" sh -lc 'test -r /home/spark/.aws/credentials && test -r /home/spark/.aws/config'
   done
   ```

   The principal must be
   `arn:aws:iam::174029311478:user/m1LakehouseUser`; every container check must
   exit zero.
3. Detach at the exact accepted commit and require a clean, unused target:

   ```bash
   git switch --detach fde426a8031a8ea101d470b54a8f0de5d4207336
   git status --short
   test ! -e benchmarks/artifacts/comparisons/phase3_baseline_20260727T035807Z_fde426a/comparison_run.json
   ```

4. Confirm `AIRFLOW_USERNAME` and `AIRFLOW_PASSWORD` exist in the launch process
   without displaying them.
5. Start the official comparison exactly once, without `--resume` or
   `--skip-evidence`:

   ```bash
   uv run python scripts/benchmarks/run_phase3_comparison.py --comparison-id phase3_baseline_20260727T035807Z_fde426a
   ```

   On any failure, preserve all partial evidence and stop for diagnosis. Never
   repair or reuse a failed official identifier.
6. After successful completion, run the canonical validator and report:

   ```bash
   uv run python scripts/benchmarks/report_phase3.py phase3_baseline_20260727T035807Z_fde426a
   ```

7. Stop for explicit report acceptance. Do not clean evidence, accept this
   comparison as canonical, or begin Phase 4 before that decision.

#### Accepted Phase 3 result

The user explicitly accepted the recovery comparison on 2026-07-27:

```text
comparison_id = phase3_baseline_20260727T035807Z_fde426a
execution_commit = fde426a8031a8ea101d470b54a8f0de5d4207336
report = docs/research_results/phase3_baseline_tradeoff_report.md
status = accepted
```

- All 166/166 comparison attempts completed; no attempt failed or retried.
- Hybrid median pipeline runtime was 139.91% to 185.43% above on-premises
  across the eight measured partitions.
- The aggregate modeled S3 request and internet-transfer cost was USD
  0.97383823. The report uses captured Amazon S3 request prices and the
  `AWSDataTransfer` first outbound tier for `us-east-1`; Cost Explorer
  reconciliation remains deferred.
- Automated completeness and cross-architecture correctness gates passed.
- Evidence cleanup was not authorized and has not been performed.

Phase 3 is complete and Phase 4 is unblocked.

### Phase 4 Execution Status

As of 2026-07-27, the partition-pruning analysis has completed against the
accepted Phase 3 measurements:

```text
analysis = docs/research_results/phase4_partition_pruning_analysis.md
source_comparison = phase3_baseline_20260727T035807Z_fde426a
status = automated analysis passed
```

- Partition filters reduced Trino physical input by 69.44% to 83.67% in every
  measured comparison.
- Median query latency improved in 28/32 architecture/protocol/partition
  comparison cases and in 13/16 hybrid cases.
- The relative hybrid latency penalty narrowed in 10/16 cases, so pruning does
  not consistently remove the hybrid penalty.
- H2 is partially supported for query-layout optimization as an absolute I/O
  mitigation. Request-count reduction was not directly measured.
- The accepted Phase 3 file-layout metrics report one data file for every
  measured partition on both architectures. The planned “fewer, larger files”
  experiment therefore has no valid treatment contrast against this baseline.

The user accepted a controlled-fragmentation/compaction design on 2026-07-27.
The second Phase 4 experiment is therefore a post-baseline 2×2 comparison:

```text
factor 1 = architecture: onprem | hybrid_aws
factor 2 = file layout: fragmented | compact
```

The design is specified under Optimization 2 below. Its next gate is
implementation plus an untimed preflight proving that all four cells have
identical logical contents and the declared file-count contrast. The accepted
Phase 3 baseline remains immutable and cannot be used as a fragmented treatment.

## Evaluation Dimensions

### 1. Performance

Measure:

- Spark staging runtime
- Spark quality-check runtime
- Spark gold aggregation runtime
- total pipeline runtime
- Trino query latency
- idempotent rerun runtime
- partition overwrite runtime

Key derived metrics:

- rows processed per second
- runtime per monthly partition
- query latency by query type
- performance delta between on-premises and hybrid profiles

### 2. Cost

Measure or estimate:

- S3 storage cost
- S3 request cost
- Glue Catalog cost where applicable
- data transfer or egress cost where applicable
- local hardware amortization estimate
- local power estimate
- runtime-driven compute cost proxy

Key derived metrics:

- cost per partition processed
- cost per million rows
- cost per query
- cost-performance ratio

### 3. Operability

Measure:

- number of services managed locally
- number of required configuration files
- number of required secrets/IAM bindings
- setup steps
- manual recovery steps
- failure diagnosis effort

Qualitative comparison:

- local storage operations burden
- AWS IAM complexity
- cloud service integration burden
- deployment repeatability

### 4. Reliability

Test:

- rerun same partition
- failed Spark job retry
- missing input partition
- partial write recovery
- Glue Catalog availability dependency
- S3 access failure behavior

Measure:

- whether retry is safe
- whether Iceberg table state remains consistent
- whether failed runs are observable
- whether benchmark artifacts capture enough evidence

### 5. Portability

Measure:

- code changes required
- config changes required
- Spark catalog configuration differences
- Trino catalog configuration differences
- table identifier stability
- vendor-specific assumptions

The target is minimal code change and explicit config/profile change. This is a supporting evaluation dimension, not a primary research question.

### 6. Security And Governance

Compare:

- local `.env` and MinIO credentials
- AWS IAM credentials
- S3 bucket policy
- Glue Catalog permissions
- encryption at rest
- encryption in transit
- auditability

This is a secondary dimension, not the main benchmark target.

## Optimization Experiments

The project will not attempt perfect optimization. It will evaluate selected practical optimizations that are relevant to RQ2.

### Optimization 1: Partition Pruning

Status:

Completed against the accepted Phase 3 measurements. This experiment is first
because it already has a valid filtered-versus-broad workload contrast.

Problem:

Hybrid object storage becomes more expensive and slower when queries scan
unnecessary files.

Experiment:

- queries with `year` and `month` filters
- broader scan queries without partition filters

Measure:

- Trino query latency
- physical input bytes
- returned rows
- partition filter presence
- runtime difference between filtered and unfiltered queries

Observed result:

Partition-aware queries reduced physical input in every case and improved
hybrid median latency in 13/16 cases. They did not consistently narrow the
relative hybrid penalty. See
`docs/research_results/phase4_partition_pruning_analysis.md`.

### Optimization 2: File-size And Small-file Control

Status:

Design accepted on 2026-07-27; implementation and untimed preflight are next.
This is explicitly a post-baseline experiment. The accepted baseline already
has one data file per measured partition, so the experiment must not be
described as improving or explaining that baseline.

Problem:

Small files are harmful for object storage and analytical query planning.

Experimental design:

- use a 2×2 design with `onprem` and `hybrid_aws` architectures crossed with
  `fragmented` and `compact` layouts
- use the same eight accepted Phase 3 monthly partitions, schemas, partition
  specification, SQL queries, compute topology, and software commit in all four
  cells
- create the fragmented treatment with 16 non-empty data files per monthly
  partition
- derive the compact treatment from the fragmented treatment and require one
  data file per monthly partition
- preserve identical rows and values across both layouts; compaction may change
  physical files and Iceberg metadata only
- use fresh Phase 4 namespaces, warehouse prefixes, run IDs, and comparison IDs;
  never mutate or reuse Phase 3 tables or evidence
- run three complete paired layout trials per architecture and partition
- retain the Phase 3 query protocols: five recorded warm executions after an
  unrecorded warm-up and three service-cold executions
- alternate which layout is queried first across paired trials and isolate
  service-cold executions with the same restart/readiness procedure to limit
  cache and ordering bias

Preflight acceptance gate:

- all 32 architecture/layout/partition cells exist
- fragmented partitions contain exactly 16 non-empty data files and compact
  partitions contain exactly one
- row counts, null counts, schema, partition values, and deterministic query
  results match between layouts and across architectures
- no timed trial begins unless the file-count contrast and logical-equivalence
  checks pass for every cell
- the frozen commit, resolved configs, infrastructure identity, and evidence
  paths are captured before the official comparison ID is used

Primary comparisons:

- within each architecture, calculate the paired fragmented-versus-compact
  latency, physical-input, planning-time, request-proxy, and write-cost deltas
- calculate the file-layout interaction by comparing the fragmentation penalty
  between `hybrid_aws` and `onprem`
- compare the hybrid-versus-on-premises latency penalty under fragmented and
  compact layouts
- report compaction runtime and write-side resource/request overhead separately
  from read-side query effects

Measure:

- number of data files
- average file size
- Spark write runtime
- Trino query latency
- Trino planning time and physical input bytes where available
- S3 request behavior in isolated evidence windows where available
- compaction CPU, memory, elapsed-time, and request proxies
- failures, retries, and result mismatches

Interpretation and permitted claims:

- a positive result may show that compaction recovers performance lost to
  deliberately induced fragmentation under the tested workload
- a larger fragmentation penalty for `hybrid_aws` supports file layout as a
  hybrid-sensitive mitigation; a similar penalty on both architectures supports
  only a general file-layout benefit
- a null or adverse result leaves the file-layout component of H2 unsupported at
  this scale
- no result may be used to claim that small files caused the accepted Phase 3
  penalty, that the production-like baseline was fragmented, or that compaction
  improved the already single-file baseline
- CloudWatch or pricing evidence that cannot be isolated to a layout-specific
  window must remain a modeled proxy and cannot support a causal request-count
  claim

Stop conditions:

- invalidate and replace an official comparison ID after any partial timed
  failure; never repair or resume it
- stop if either layout misses its declared file count, logical results differ,
  cache isolation fails, or concurrent external traffic contaminates the
  evidence window
- stop for explicit report acceptance before treating the file-layout result as
  canonical or beginning the optional executor-sizing experiment

### Optional Optimization 3: Spark Executor Sizing

Problem:

More local parallelism may not improve hybrid performance if the bottleneck becomes remote object storage or network throughput.

Experiment:

- small Spark profile
- medium Spark profile
- current/default Spark profile

Measure:

- runtime
- stability
- resource use
- diminishing returns

This experiment is optional and should only be included if time remains after the first two optimizations.

## Workloads

Use the NYC TLC workload already implemented in the project.

Minimum required workloads:

```text
yellow 2011-01
green 2014-01
```

Recommended benchmark levels:

```text
smoke: one yellow month and one green month
monthly baseline: selected representative months
idempotency: rerun same partition
query suite: fixed Trino analytical queries
optimization: same partitions under tuned settings
```

Do not expand workload volume until the benchmark harness is stable.

## Measurement Rules

To keep the study defensible:

- Use the same workload definitions for on-premises and hybrid.
- Use the same number of repetitions.
- Capture raw metrics and normalized metrics separately.
- Do not compare one-off runs.
- Do not claim cold-cache behavior unless services are explicitly restarted.
- Do not compare Glue ETL against local Spark in the main baseline.
- Do not treat S3 storage price alone as total cost.
- Record all config hashes and git SHAs.

## Expected Deliverables

### Engineering Deliverables

- reproducible benchmark harness
- on-premises benchmark profile
- AWS S3 + Glue benchmark profile
- AWS infrastructure definition or setup notes
- Trino query suite
- benchmark metrics table
- JSON benchmark artifacts

### Research Deliverables

- on-premises baseline results
- hybrid baseline results
- tradeoff matrix
- optimization results
- architecture diagram
- cost model
- limitations section
- final thesis/report discussion

## Proposed Timeline

### Phase 1: Benchmark Harness And On-Premises Baseline

Goal:

Make the current MVP measurable.

Tasks:

- implement benchmark runner
- implement benchmark DAG
- add metrics schema
- add query suite
- run on-premises smoke benchmarks
- run on-premises idempotency tests

Output:

- trusted on-premises baseline results

### Phase 2: Hybrid Storage Baseline

Goal:

Move storage/catalog to AWS while keeping compute local.

Tasks:

- create S3 bucket and prefixes
- create Glue databases
- add AWS Spark/Iceberg catalog profile
- upload raw sample data to S3
- run one yellow and one green partition
- verify Glue table registration
- verify Spark and Trino read paths

Output:

- working S3 + Iceberg + Glue baseline

### Phase 3: Comparative Benchmarking

Goal:

Run identical workloads on both architectures.

Tasks:

- run on-premises benchmark suite
- run hybrid benchmark suite
- collect runtime, query, cost, and reliability evidence
- compare raw and normalized metrics

Output:

- baseline tradeoff analysis

### Phase 4: Optimization Experiments

Goal:

Evaluate practical mitigations for hybrid-storage overhead.

Tasks:

- analyze the accepted filtered-versus-broad partition-pruning measurements
- implement the accepted 2×2 controlled-fragmentation/compaction harness
- pass the untimed file-count and logical-equivalence preflight
- run the file-layout comparison under a fresh frozen commit and identifier
- validate and explicitly accept the file-layout report
- optionally run Spark executor-sizing experiment
- compare optimized vs unoptimized hybrid results

Output:

- optimization impact analysis

### Phase 5: Thesis Write-up

Goal:

Turn results into a defensible research narrative.

Tasks:

- summarize architecture tradeoffs
- explain measurement limitations
- report optimization outcomes
- document future work

Output:

- final thesis/report material

## Success Criteria

The research is successful if it can answer:

1. What performance, cost, reliability, and operability tradeoffs appeared after moving storage/catalog to AWS?
2. Which tradeoffs were beneficial and which were negative?
3. Did file-size control reduce hybrid query latency or request overhead?
4. Did partition-aware queries reduce hybrid query latency or request overhead?
5. Did increased Spark parallelism improve ingestion, and where did diminishing returns appear?
6. Which results are strong enough to support the hypotheses, and which remain inconclusive?

The research does not need to prove that hybrid is universally better.

## Key Risks

### Risk 1: Too Many Variables

If compute is moved to AWS too early, the study cannot isolate storage/catalog tradeoffs.

Mitigation:

Keep Spark and Trino local for the main baseline.

### Risk 2: S3 Tables Confusion

S3 Tables may be mistaken for a replacement for Iceberg.

Mitigation:

Treat S3 Tables as future managed-Iceberg storage, not the main baseline.

### Risk 3: Cost Oversimplification

S3 storage cost alone may look cheap but total workload cost may increase.

Mitigation:

Track request cost, transfer cost, runtime impact, and operational complexity.

### Risk 4: Over-optimization

Trying to perfectly tune Spark, Iceberg, and Trino can consume the project.

Mitigation:

Limit optimization experiments to file-size control, partition pruning, and optional executor sizing.

## Assumptions

- The research window is approximately 2 months.
- AWS account access and budget are available for small benchmark workloads.
- The main AWS region is `us-east-1` unless changed later.
- The table format remains Apache Iceberg v2.
- Local compute remains the main compute path.
- The benchmark harness is implemented before hybrid benchmarking.
- Existing job CLIs and table identifiers remain stable.
- Future ideas should modify this file first.

