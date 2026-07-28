# H3 Spark Executor-Sizing Report

- Comparison ID: `h3_executor_sizing_20260728T131218Z_96aedac_official03`
- Commit SHA: `96aedac4c8977727d239441aee1f2e79d7385268`
- Status: **ACCEPTED 2026-07-28**

## Pipeline Runtime

| Workload | Profile | Cores | n | Runs (s) | Median (s) |
| --- | --- | ---: | ---: | --- | ---: |
| green_2014_01 | default | 12 | 3 | 98.669, 99.330, 102.235 | 99.330 |
| green_2014_01 | medium | 8 | 3 | 94.333, 100.396, 100.007 | 100.007 |
| green_2014_01 | small | 4 | 3 | 94.015, 94.118, 96.062 | 94.118 |
| yellow_2011_01 | default | 12 | 3 | 162.819, 156.796, 165.992 | 162.819 |
| yellow_2011_01 | medium | 8 | 3 | 162.503, 161.317, 175.703 | 162.503 |
| yellow_2011_01 | small | 4 | 3 | 156.820, 159.632, 163.489 | 159.632 |

## Diminishing Returns

- green_2014_01: 4→8 cores changed median runtime by -6.26%; 8→12 cores changed it by 0.68%.
- yellow_2011_01: 4→8 cores changed median runtime by -1.80%; 8→12 cores changed it by -0.19%.

## Interpretation Limits

- This is a three-repetition experiment on two declared monthly partitions.
- It isolates application core allocation on the existing 12-core cluster; it does not vary worker hardware, executor memory, network capacity, or S3 configuration.
- Runtime and Spark History metrics support a local diminishing-return claim only; they do not identify network or object storage as the causal bottleneck.

## Acceptance Gate

- The user explicitly accepted this report on 2026-07-28.
- H3 is canonical and Phase 4 experimental work is complete.
