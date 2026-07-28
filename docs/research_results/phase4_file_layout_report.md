# Phase 4 File-Layout Experiment Report

- Comparison ID: `phase4_file_layout_20260727T102700Z_f62a1f5_official01`
- Commit SHA: `f62a1f5dbe979c3f77b88dc84437ac8552fa0045`
- Comparison hash: `889ede6122b819ba1778260732c0929c84cbf8c33c6d27f16e9dce5c8b762ddf`
- Preflight ID: `phase4_file_layout_20260727_f62a1f5`
- Status: **ACCEPTED 2026-07-28**

## Read-Side Effects

Positive fragmentation penalties mean fragmented files were slower or more expensive than compact files. Positive hybrid penalties mean hybrid was slower or more expensive than on-premises. The interaction is the hybrid fragmentation penalty minus the on-premises fragmentation penalty.

| Protocol/query/measure | Complete pairs | On-prem fragmentation | Hybrid fragmentation | Interaction (pp) | Hybrid penalty fragmented | Hybrid penalty compact |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| service_cold_recorded:01_partition_financial_aggregation:duration_seconds | 72 | 53.05% [50.40, 59.43] | -9.70% [-13.22, -7.93] | -73.10 pp [-76.92, -68.08] | 280.47% [260.26, 298.77] | 632.99% [564.59, 683.30] |
| service_cold_recorded:01_partition_financial_aggregation:physical_input_bytes | 72 | 216.58% [-1.73, 434.90] | 216.75% [-1.39, 434.90] | 0.00 pp [0.00, 0.00] | 0.00% [0.00, 0.00] | -0.02% [-0.04, 0.00] |
| service_cold_recorded:01_partition_financial_aggregation:planning_time_ms | 72 | 0.00% [-3.85, 1.96] | 0.00% [-3.81, 2.02] | -1.74 pp [-6.88, 4.19] | -13.46% [-15.48, -12.25] | -11.92% [-14.81, -8.08] |
| service_cold_recorded:02_pickup_location_aggregation:duration_seconds | 72 | 55.11% [52.44, 58.50] | -6.56% [-17.61, -3.97] | -64.03 pp [-70.86, -60.30] | 257.92% [239.30, 267.75] | 507.34% [494.34, 523.92] |
| service_cold_recorded:02_pickup_location_aggregation:physical_input_bytes | 72 | 433.41% [-6.15, 872.98] | 433.68% [-5.62, 872.98] | 0.05 pp [0.00, 0.11] | 0.00% [0.00, 0.00] | -0.03% [-0.06, 0.00] |
| service_cold_recorded:02_pickup_location_aggregation:planning_time_ms | 72 | 0.00% [-3.45, 3.26] | 2.06% [-2.08, 4.17] | 0.17 pp [-3.87, 6.66] | -12.85% [-14.90, -11.57] | -15.03% [-19.67, -11.37] |
| service_cold_recorded:03_dataset_financial_scan:duration_seconds | 18 | 36.33% [18.11, 61.21] | -3.13% [-22.17, 19.00] | -41.48 pp [-59.84, -35.44] | 430.50% [325.10, 489.41] | 597.98% [495.26, 766.60] |
| service_cold_recorded:03_dataset_financial_scan:physical_input_bytes | 18 | 220.85% [-1.98, 443.67] | 221.03% [-1.62, 443.67] | 0.18 pp [0.00, 0.36] | -0.01% [-0.02, 0.00] | -0.19% [-0.38, 0.00] |
| service_cold_recorded:03_dataset_financial_scan:planning_time_ms | 18 | -5.70% [-19.00, 3.57] | -1.85% [-15.62, 12.38] | 3.22 pp [-9.38, 21.20] | 4.00% [-1.58, 10.42] | 3.92% [-12.30, 9.53] |
| warm_recorded:01_partition_financial_aggregation:duration_seconds | 120 | -2.47% [-4.85, 1.75] | -19.51% [-28.26, -7.05] | -16.65 pp [-23.90, -7.35] | 2989.18% [2850.51, 3176.59] | 4061.25% [3616.40, 4290.01] |
| warm_recorded:01_partition_financial_aggregation:physical_input_bytes | 120 | 216.58% [-1.73, 434.90] | 216.75% [-1.39, 434.90] | 0.00 pp [0.00, 0.00] | 0.00% [0.00, 0.00] | -0.02% [-0.04, 0.00] |
| warm_recorded:01_partition_financial_aggregation:planning_time_ms | 120 | 0.00% [0.00, 10.56] | 0.00% [-8.33, 9.17] | -8.21 pp [-15.00, 0.00] | 0.00% [0.00, 12.50] | 12.50% [0.00, 16.67] |
| warm_recorded:02_pickup_location_aggregation:duration_seconds | 120 | -2.93% [-6.66, -0.48] | -8.21% [-12.12, 13.78] | 8.11 pp [-3.95, 19.07] | 2333.91% [2096.33, 2462.26] | 2142.19% [1973.49, 2442.21] |
| warm_recorded:02_pickup_location_aggregation:physical_input_bytes | 120 | 433.41% [-6.15, 872.98] | 433.68% [-5.62, 872.98] | 0.05 pp [0.00, 0.11] | 0.00% [0.00, 0.00] | -0.03% [-0.06, 0.00] |
| warm_recorded:02_pickup_location_aggregation:planning_time_ms | 120 | 0.00% [-7.69, 0.00] | 0.00% [0.00, 9.09] | 7.38 pp [0.00, 12.50] | 10.00% [0.00, 14.29] | 0.00% [0.00, 10.56] |
| warm_recorded:03_dataset_financial_scan:duration_seconds | 30 | 43.72% [37.62, 62.80] | 11.90% [-20.62, 43.67] | -32.37 pp [-56.05, -6.81] | 3101.84% [2702.00, 4062.90] | 4199.67% [4052.11, 4627.94] |
| warm_recorded:03_dataset_financial_scan:physical_input_bytes | 30 | 220.85% [-1.98, 443.67] | 221.03% [-1.62, 443.67] | 0.18 pp [0.00, 0.36] | -0.01% [-0.02, 0.00] | -0.19% [-0.38, 0.00] |
| warm_recorded:03_dataset_financial_scan:planning_time_ms | 30 | 0.00% [-19.44, 12.50] | 0.00% [0.00, 14.29] | 0.83 pp [-13.39, 19.44] | 0.00% [0.00, 20.00] | 0.00% [-11.81, 6.25] |

Each bracket is a deterministic 95% bootstrap interval for the median using 10,000 resamples. Warm-recorded and service-cold results are kept separate. No p-value or universal performance claim is made.

## Write-Side And Request Evidence

- The frozen preflight was validation-only and contains no timed preparation jobs. Compaction runtime, write resource use, and write cost are unavailable for the official comparison.
- The comparison runner did not create isolated S3 request-metric windows. No causal request-count or request-cost claim is permitted.
- Physical input bytes are Trino query-engine evidence, not an S3 API request count.

## Permitted Interpretation

- A positive hybrid fragmentation effect may support compaction as a mitigation for deliberately induced fragmentation in this workload.
- A similar fragmentation effect on both architectures supports only a general file-layout benefit.
- These results cannot show that small files caused the accepted Phase 3 penalty or that compaction improved the single-file Phase 3 baseline.

## Acceptance Gate

- All automated completeness, identity, success, and correctness gates passed.
- The user explicitly accepted this comparison on 2026-07-28.
- The file-layout result is canonical. The H3 executor-sizing experiment may begin.
