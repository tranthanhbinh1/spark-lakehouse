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

### Optimization 1: File-size And Small-file Control

Problem:

Small files are harmful for object storage and analytical query planning.

Experiment:

- default write behavior
- controlled repartitioning or target file-size strategy

Measure:

- number of data files
- average file size
- Spark write runtime
- Trino query latency
- S3 request behavior where available

Expected result:

Fewer, larger files should reduce metadata and object-store overhead, but may increase write-side shuffle cost.

### Optimization 2: Partition Pruning

Problem:

Hybrid object storage becomes more expensive and slower when queries scan unnecessary files.

Experiment:

- queries with `year` and `month` filters
- broader scan queries without partition filters

Measure:

- Trino query latency
- returned rows
- partition filter presence
- runtime difference between filtered and unfiltered queries

Expected result:

Partition-aware queries should reduce latency and unnecessary object-store reads.

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

- run file-size/small-file optimization experiment
- run partition-pruning experiment
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
