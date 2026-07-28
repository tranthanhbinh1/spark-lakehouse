# Phase 4 File-Layout Experiment: Preliminary Progress Report

Status: **PRELIMINARY — RUN IN PROGRESS — NOT ELIGIBLE FOR ACCEPTANCE**

Snapshot time: `2026-07-27T22:10:27+07:00`

## Purpose

This document starts the Phase 4 report before the official comparison
finishes. It records the frozen design, evidence locations, preflight result,
and a point-in-time execution snapshot. It deliberately does not calculate or
interpret fragmented-versus-compact treatment effects from the incomplete run.

The final result remains subject to the automated completeness, identity,
success, artifact/database consistency, and result-equivalence gates in
`scripts/benchmarks/report_phase4_file_layout.py`.

## Frozen Experiment Identity

- Execution commit:
  `f62a1f5dbe979c3f77b88dc84437ac8552fa0045`
- Comparison ID:
  `phase4_file_layout_20260727T102700Z_f62a1f5_official01`
- Preflight ID: `phase4_file_layout_20260727_f62a1f5`
- Design: `onprem` and `hybrid_aws` crossed with `fragmented` and
  `compact`
- Fragmented treatment: exactly 16 non-empty data files per monthly partition
- Compact treatment: exactly one data file per monthly partition
- Trials: three
- Query targets: 18
- Execution protocols per target and trial: one unrecorded warm-up, five
  recorded warm executions, and three recorded service-cold executions
- Total paired steps: 486
- Total individual cell executions: 1,944

## Preflight Result

The frozen validation-only preflight passed all declared entry gates:

- 32 of 32 architecture/layout/partition cells observed
- all fragmented partitions contained exactly 16 non-empty files
- all compact partitions contained exactly one file
- schemas, row counts, null counts, checksums, partition values, and
  deterministic query results matched
- frozen worktree was clean
- infrastructure snapshots covered all four cells and contained no errors

Preflight artifact:

`benchmarks/artifacts/phase4_preflight/phase4_file_layout_20260727_f62a1f5/preflight.json`

## Evidence Storage

| Evidence | Location | Persistence |
| --- | --- | --- |
| Comparison state and schedule progress | `benchmarks/artifacts/comparisons/phase4_file_layout_20260727T102700Z_f62a1f5_official01/comparison_run.json` | Local JSON; updated after every state transition |
| Per-execution query results, metrics, and snapshots | `benchmarks/artifacts/comparisons/phase4_file_layout_20260727T102700Z_f62a1f5_official01/benchmarks/<benchmark_run_id>/` | Local JSON; one directory per execution |
| Recorded query metrics | `lakehouse.benchmark.run_metrics` filtered by the comparison ID | Iceberg table queried through Trino |
| Frozen layout and logical-equivalence evidence | Preflight artifact above | Local JSON |
| Final raw report export | `benchmarks/artifacts/comparisons/phase4_file_layout_20260727T102700Z_f62a1f5_official01/report/` | Generated after completion |
| Final research report | `docs/research_results/phase4_file_layout_report.md` | Generated after completion and committed only after acceptance |

`benchmarks/artifacts/` is intentionally ignored by Git. The raw local
artifacts therefore exist on disk but are not protected by repository history.
The Iceberg metrics table is the second persisted copy of recorded query
metrics. Warm-up queries remain in local artifacts only because their database
metric insertion is intentionally skipped.

## Progress Snapshot

At the snapshot time:

- comparison state: `running`
- runner process: active
- completed individual executions: 1,311 of 1,944 (67.44%)
- completed paired steps: 327 of 486
- failed paired steps: 0
- persisted per-execution artifacts: 1,311
- database metrics: 1,163
  - recorded warm metrics: 731
  - recorded service-cold metrics: 432
  - unsuccessful database metrics: 0
  - metrics missing `file_layout`: 0
- warm-up artifacts without database insertion: 148
- trials 1 and 2: complete
- trial 3: in progress

These counts are a point-in-time snapshot and will become stale while the
runner continues. Use:

```bash
uv run python scripts/benchmarks/check_phase4_status.py
```

for current progress.

## Preliminary Interpretation Boundary

No performance conclusion is reported yet. Calculating treatment effects before
trial 3 finishes would substitute an incomplete sample for the frozen design
and risks order and coverage bias.

In particular, this report does not yet claim:

- that compaction improves latency, planning time, or physical input
- that fragmentation affects hybrid storage more than on-premises storage
- that small files caused the accepted Phase 3 hybrid penalty
- that S3 request counts or request costs changed

The runner does not isolate S3 request-metric windows, so the final report also
cannot make a causal request-count or request-cost claim. Physical input bytes
are Trino engine evidence, not S3 API request counts.

## Work Remaining

1. Let trial 3 and the full 1,944-execution schedule finish.
2. Confirm state `complete`, 486 complete pairs, 1,944 complete members, and no
   failed attempt.
3. Run:

   ```bash
   uv run python scripts/benchmarks/report_phase4_file_layout.py \
     phase4_file_layout_20260727T102700Z_f62a1f5_official01
   ```

4. Review the validator output and final report. If any automated gate fails,
   do not accept the result.
5. Present the validated file-layout result for explicit user acceptance.
6. Start the H3 Spark executor-sizing experiment only after that acceptance.
