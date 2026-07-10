# Phase 3 Comparative Benchmark Plan

## Relationship To The Canonical Plan

This document is subordinate to docs/research_plan/hybrid_storage_tradeoff_research_plan.md.
It defines the execution gate for the first accepted on-premises versus hybrid comparison.
Phase 4 optimization work must not begin until this plan produces an accepted report.

## Research Boundary

Phase 3 answers RQ1 using performance measurements, marginal AWS cost exposure and local
resource proxies, operability evidence, and a qualitative infrastructure-resilience and
responsibility-boundary assessment.

Reliability is not treated as a symmetric hardware-failure experiment. The current
on-premises baseline is a self-managed MinIO service, while S3 and Glue are managed services
with different failure domains. The report must distinguish measured, estimated, and
architectural evidence and must not produce a single weighted winner score.

## Workload And Isolation

Use eight fixed partitions: yellow 2011-01, 2011-04, 2011-07, 2011-10 and green 2014-01,
2014-04, 2014-07, 2014-10.

Seed identical source files into isolated raw prefixes and record SHA-256, byte-size,
destination key, and object metadata for every file. Use dedicated catalog namespaces in
both environments: phase3_silver, phase3_quality, and phase3_gold.

Hybrid warehouse data must be placed below warehouse/phase3/. Existing smoke, operational,
and production-like tables must remain untouched.

## Comparison Protocol

- Run three pipeline repetitions per partition and architecture.
- Alternate architecture order for paired trials: on-premises/hybrid, then hybrid/on-premises.
- Run correctness queries once after all pipeline writes.
- Run partition financial aggregation, pickup-location aggregation, and a dataset-wide
  financial scan as performance queries.
- Run one unrecorded warm-up followed by five recorded warm executions for each performance
  target.
- Run three service-cold executions for yellow January, green January, and each dataset-wide
  scan. Restart the Trino coordinator and both workers before every cold execution and
  verify all three nodes are ready.
- Preserve failed or invalid trials. If a pair is contaminated, rerun both members of the
  pair; never replace only one result.
- Do not remove statistical outliers.

Dataset-wide scans are valid only because the dedicated Phase 3 namespaces contain exactly
the selected partitions. The existing duplicate filtered query must not be used.

## Required Implementation

Add optional Silver, Quality, and Gold namespace arguments to the Spark jobs, with current
namespace defaults preserved. Pass those values through both benchmark DAGs and Phase 3
profiles.

Add a comparison specification and runner with dry-run and resume modes. The runner must
reject non-clean Git worktrees for official runs and record commit SHA, configuration hash,
environment snapshot, comparison ID, trial ID, sequence position, and measurement protocol.

Extend lakehouse.benchmark.run_metrics with comparison/trial identity, measurement protocol,
retry count, Trino queued/planning/CPU time, physical input bytes, peak memory, and generic
metric name/value/unit fields. Add metric types for object-store observations, cost
estimates, and local resource proxies.

Add a preparation command that creates the isolated namespaces/tables and seeds both object
stores. It must support dry-run, checksum validation, and explicit non-destructive cleanup
only after the report is accepted.

## Cost And Resilience Evidence

For AWS, collect prefix-filtered S3 CloudWatch request, error, byte-transfer, and storage
metrics at pipeline, warm-query, and cold-query block level. Capture a dated pricing
snapshot and reconcile later with Cost Explorer. Do not claim per-query S3 request cost:
request metrics are delayed, one-minute, and best-effort.

For on-premises, report runtime, vCPU-hours, memory-hours, and storage bytes; do not convert
them to USD without measured power or hardware-cost inputs.

The resilience section is qualitative. Compare failure domains, redundancy, recovery
ownership, backup/versioning, monitoring, encryption, auditability, network dependency, and
managed-service responsibility. Do not inject outages.

## Analysis And Deliverables

Produce raw JSON/CSV artifacts under ignored benchmark artifacts and commit a curated report
at docs/research_results/phase3_baseline_tradeoff_report.md. The report must include raw
samples, median, IQR, p95, paired percentage deltas, and deterministic bootstrap 95% intervals.
No p-value or significance claims are permitted.

The report must identify both architecture run IDs, configuration hashes, the commit SHA,
protocol deviations, cost limitations, resilience evidence, and unanswered questions. The
canonical plan is updated with accepted run IDs only after all gates pass.

## Acceptance Gates

- python3 -m py_compile, uv run ruff check ., and focused uv run pyrefly check pass.
- Workload/profile/query dry-runs pass for both architectures.
- Raw checksums and byte sizes match across object stores.
- Row counts match exactly; monetary outputs agree after cent rounding and distance/duration
  outputs agree to six decimals.
- All paired pipeline and query samples complete successfully with stable repeated-write
  row counts.
- Environment snapshots, Spark application IDs, query IDs, metric IDs, and metric insertion
  results are complete and retry-safe.
- AWS cost observations and the qualitative resilience matrix are attached to the report.
- The tracked report is accepted before Phase 4 begins.

## Assumptions

- Spark 3.5.6, Trino 480, local compute, and the locked S3 + Iceberg + Glue architecture
  remain unchanged.
- Official execution starts only from a committed, clean worktree.
- CloudWatch request metrics are disabled after collection to avoid ongoing monitoring cost.
- No optimization experiment or executor-sizing comparison is included in Phase 3.
