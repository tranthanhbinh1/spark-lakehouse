# Phase 3 Execution And Baseline Acceptance Plan

## Relationship To The Canonical Plan

This document is subordinate to docs/research_plan/hybrid_storage_tradeoff_research_plan.md.
Phase 4 optimization work must not begin until this plan produces an accepted report.

## Summary

Complete and execute Phase 3 before beginning optimization experiments. The phase ends when
the comparative report is manually accepted and the canonical plan records the accepted
comparison ID, run IDs, configuration hash, and commit SHA.

Current state: all eight source files and required containers are available, but the
implementation is uncommitted, the live metrics table needs migration, focused Pyrefly has
two Pandera typing failures, and cost/report tooling is incomplete.

## Implementation Changes

- Harden the comparison specification with explicit query targets. Each target defines its
  SQL file, workload, warm-up count, five recorded executions, and three cold executions
  where applicable. Dataset-wide scans use one yellow and one green target rather than
  repeating once per partition.
- Alternate architectures for every recorded pipeline, warm-query, and cold-query pair.
  Assign each pair an attempt number and unique run IDs.
- Treat any failed or contaminated member as invalidating the whole pair. Resume reruns both
  members, preserves invalid attempts, increments retry_count, and never silently reuses
  the successful half.
- Remove dirty-worktree overrides from official execution. Record the full commit SHA,
  comparison hash, per-run configuration hash, timestamps, environment snapshots, sequence
  position, and protocol in comparison state.
- Restart the Trino coordinator and both workers before each cold trial, then poll until all
  three nodes are active and a bounded readiness query succeeds.

### Preparation And Isolation

- Migrate the existing lakehouse.benchmark.run_metrics table idempotently with the Phase 3
  comparison, timing, memory, and generic metric columns. Verify the live schema before
  execution.
- Upload the eight source files to the isolated raw prefixes. Stream each stored object back
  to calculate its actual SHA-256; do not infer remote correctness from the local checksum or
  ETag.
- Record source path, object URI, local and remote SHA-256, size, ETag, version ID,
  modification time, and metadata in the preparation manifest.
- Create dedicated Silver, Quality, and Gold namespaces. Set and verify Glue database
  locations so every hybrid table and metadata path remains below
  s3://.../warehouse/phase3/.
- Make cleanup manifest-scoped. It may delete only recorded Phase 3 objects and namespaces,
  requires an accepted-report marker, and remains unused until Phase 4 no longer needs the
  baseline.

### Evidence Collection

- Enable temporary S3 request-metric configurations for phase3/raw and warehouse/phase3,
  using comparison-specific filter IDs.
- Record 60-second CloudWatch request, error, and byte-transfer datapoints for pipeline,
  warm-query, and cold-query windows. Poll for delayed datapoints without rerunning completed
  trials.
- Record exact prefix storage bytes/object counts through object listing and a dated
  bucket-level CloudWatch storage snapshot. Explicitly document that CloudWatch storage
  metrics are not precise per-prefix, per-block evidence.
- Capture a dated AWS pricing response and calculate only aggregate block-level marginal
  estimates. Reconcile later against daily Cost Explorer service totals; never claim
  per-query S3 cost.
- Sample Docker CPU and memory every five seconds around each block. Integrate CPU-seconds
  and GiB-hours, and report configured capacity-hours separately. Do not translate local
  proxies into USD.
- Generate a qualitative resilience matrix covering failure domains, redundancy, recovery
  ownership, backup/versioning, monitoring, encryption, auditability, network dependency,
  and service responsibility. Do not inject failures.

## Execution And Analysis

1. Fix focused Pyrefly failures, run static checks and both architecture dry-runs, then
   commit the complete harness.
2. Run live preparation and verify remote checksums, namespace locations, table schemas,
   Airflow authentication, Spark History lookup, and Trino readiness.
3. Run a non-evidence preflight using yellow January and green January. Fixing harness or
   protocol behavior after this point requires a new clean commit.
4. Execute the official three paired pipeline repetitions, correctness pass, warm-query
   pairs, and service-cold pairs from the clean commit.
5. Validate exact row counts and quality summaries. Compare monetary values after cent
   rounding and distance/duration values to six decimals.
6. Verify metric completeness, unique IDs, Spark application IDs, Trino query IDs, stable
   repeated-write row counts, environment snapshots, and retry-safe database insertion.
7. Generate raw JSON/CSV plus docs/research_results/phase3_baseline_tradeoff_report.md from
   lakehouse.benchmark.run_metrics and retained artifacts.
8. Report raw samples, median, linear-interpolation IQR and p95, paired percentage deltas,
   and 10,000 deterministic paired bootstrap resamples with seed 20260713 and percentile
   95% intervals. Make no p-value or significance claims.
9. Disable temporary S3 request metrics immediately after collection. Preserve benchmark
   data until manual report acceptance.
10. After explicit user approval, update the canonical plan with accepted identifiers and
    commit the report and plan update. Phase 4 remains blocked until this commit.

## Test And Acceptance Plan

- python3 -m py_compile passes for every changed Python file.
- uv run ruff check . passes.
- Focused uv run pyrefly check passes for the Phase 3 runner, preparation, benchmark
  client/DAG, and touched ETL paths. Existing unrelated simulation-DAG typing debt remains
  out of scope.
- Preparation and comparison dry-runs produce deterministic schedules and manifests for
  both architectures.
- Tests cover query-target expansion, architecture alternation, pair invalidation, resume,
  retry IDs, missing CloudWatch datapoints, remote checksum mismatch, unsafe cleanup, and
  report statistics.
- A metric reload test proves reinsertion leaves one row per metric_id.
- No trial is accepted when artifacts, database rows, identifiers, correctness results, or
  required evidence are incomplete.

## Assumptions And Defaults

- Phase 3 retains the locked local-compute versus S3/Glue architecture and eight-partition
  workload.
- Existing boto3 and pandas dependencies are sufficient; no new analysis dependency is
  required.
- Airflow credentials are exported in the shell launching the official runner.
- CloudWatch request metrics are delayed and best-effort; missing evidence pauses acceptance
  but does not force a benchmark rerun.
- Manual user signoff is required before canonical-plan updates or data cleanup.
