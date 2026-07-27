# Phase 3 Baseline Tradeoff Report

- Comparison ID: `phase3_baseline_20260727T035807Z_fde426a`
- Commit SHA: `fde426a8031a8ea101d470b54a8f0de5d4207336`
- Comparison hash: `a5de3b9d94e73d4a2c541f56de875714d080e95393fec092e8427321b0f68b63`
- Status: **ACCEPTED 2026-07-27**

## Performance Summary

| Measurement | n | On-prem median/IQR/p95 (s) | Hybrid median/IQR/p95 (s) | Paired delta | 95% bootstrap interval |
| --- | ---: | ---: | ---: | ---: | ---: |
| correctness_once:query:01_partition_row_count:green:2014:1 | 1 | 0.048570 / 0.000000 / 0.048570 | 0.372062 / 0.000000 / 0.372062 | 666.04% | [666.04%, 666.04%] |
| correctness_once:query:01_partition_row_count:green:2014:10 | 1 | 0.043010 / 0.000000 / 0.043010 | 0.388566 / 0.000000 / 0.388566 | 803.43% | [803.43%, 803.43%] |
| correctness_once:query:01_partition_row_count:green:2014:4 | 1 | 0.041755 / 0.000000 / 0.041755 | 0.365068 / 0.000000 / 0.365068 | 774.31% | [774.31%, 774.31%] |
| correctness_once:query:01_partition_row_count:green:2014:7 | 1 | 0.044311 / 0.000000 / 0.044311 | 0.372893 / 0.000000 / 0.372893 | 741.54% | [741.54%, 741.54%] |
| correctness_once:query:01_partition_row_count:yellow:2011:1 | 1 | 0.047982 / 0.000000 / 0.047982 | 0.422642 / 0.000000 / 0.422642 | 780.83% | [780.83%, 780.83%] |
| correctness_once:query:01_partition_row_count:yellow:2011:10 | 1 | 0.046092 / 0.000000 / 0.046092 | 0.390991 / 0.000000 / 0.390991 | 748.28% | [748.28%, 748.28%] |
| correctness_once:query:01_partition_row_count:yellow:2011:4 | 1 | 0.045970 / 0.000000 / 0.045970 | 0.370318 / 0.000000 / 0.370318 | 705.56% | [705.56%, 705.56%] |
| correctness_once:query:01_partition_row_count:yellow:2011:7 | 1 | 0.043552 / 0.000000 / 0.043552 | 0.357257 / 0.000000 / 0.357257 | 720.30% | [720.30%, 720.30%] |
| correctness_once:query:02_quality_result_summary:green:2014:1 | 1 | 0.065020 / 0.000000 / 0.065020 | 0.986112 / 0.000000 / 0.986112 | 1416.63% | [1416.63%, 1416.63%] |
| correctness_once:query:02_quality_result_summary:green:2014:10 | 1 | 0.115512 / 0.000000 / 0.115512 | 0.982645 / 0.000000 / 0.982645 | 750.69% | [750.69%, 750.69%] |
| correctness_once:query:02_quality_result_summary:green:2014:4 | 1 | 0.062147 / 0.000000 / 0.062147 | 0.963441 / 0.000000 / 0.963441 | 1450.26% | [1450.26%, 1450.26%] |
| correctness_once:query:02_quality_result_summary:green:2014:7 | 1 | 0.067512 / 0.000000 / 0.067512 | 0.969625 / 0.000000 / 0.969625 | 1336.23% | [1336.23%, 1336.23%] |
| correctness_once:query:02_quality_result_summary:yellow:2011:1 | 1 | 0.066753 / 0.000000 / 0.066753 | 3.084745 / 0.000000 / 3.084745 | 4521.12% | [4521.12%, 4521.12%] |
| correctness_once:query:02_quality_result_summary:yellow:2011:10 | 1 | 0.068066 / 0.000000 / 0.068066 | 1.063760 / 0.000000 / 1.063760 | 1462.82% | [1462.82%, 1462.82%] |
| correctness_once:query:02_quality_result_summary:yellow:2011:4 | 1 | 0.062688 / 0.000000 / 0.062688 | 1.014130 / 0.000000 / 1.014130 | 1517.75% | [1517.75%, 1517.75%] |
| correctness_once:query:02_quality_result_summary:yellow:2011:7 | 1 | 0.063399 / 0.000000 / 0.063399 | 0.988645 / 0.000000 / 0.988645 | 1459.41% | [1459.41%, 1459.41%] |
| correctness_once:query:03_gold_revenue_check:green:2014:1 | 1 | 0.044083 / 0.000000 / 0.044083 | 0.844356 / 0.000000 / 0.844356 | 1815.38% | [1815.38%, 1815.38%] |
| correctness_once:query:03_gold_revenue_check:green:2014:10 | 1 | 0.075975 / 0.000000 / 0.075975 | 0.611562 / 0.000000 / 0.611562 | 704.95% | [704.95%, 704.95%] |
| correctness_once:query:03_gold_revenue_check:green:2014:4 | 1 | 0.042470 / 0.000000 / 0.042470 | 0.645132 / 0.000000 / 0.645132 | 1419.03% | [1419.03%, 1419.03%] |
| correctness_once:query:03_gold_revenue_check:green:2014:7 | 1 | 0.042620 / 0.000000 / 0.042620 | 0.675741 / 0.000000 / 0.675741 | 1485.51% | [1485.51%, 1485.51%] |
| correctness_once:query:03_gold_revenue_check:yellow:2011:1 | 1 | 0.042585 / 0.000000 / 0.042585 | 0.639774 / 0.000000 / 0.639774 | 1402.35% | [1402.35%, 1402.35%] |
| correctness_once:query:03_gold_revenue_check:yellow:2011:10 | 1 | 0.048082 / 0.000000 / 0.048082 | 0.668986 / 0.000000 / 0.668986 | 1291.35% | [1291.35%, 1291.35%] |
| correctness_once:query:03_gold_revenue_check:yellow:2011:4 | 1 | 0.044730 / 0.000000 / 0.044730 | 0.637552 / 0.000000 / 0.637552 | 1325.32% | [1325.32%, 1325.32%] |
| correctness_once:query:03_gold_revenue_check:yellow:2011:7 | 1 | 0.045298 / 0.000000 / 0.045298 | 0.654858 / 0.000000 / 0.654858 | 1345.67% | [1345.67%, 1345.67%] |
| pipeline_paired:pipeline:green:2014:1 | 3 | 35.732314 / 0.498880 / 36.367931 | 98.817846 / 5.946996 / 108.412345 | 175.34% | [171.19%, 206.38%] |
| pipeline_paired:pipeline:green:2014:10 | 3 | 38.124179 / 0.764090 / 38.189615 | 104.989965 / 1.944365 / 108.012400 | 183.66% | [174.00%, 186.32%] |
| pipeline_paired:pipeline:green:2014:4 | 3 | 37.116023 / 0.448825 / 37.292594 | 105.329587 / 1.648612 / 107.043884 | 185.43% | [183.78%, 187.40%] |
| pipeline_paired:pipeline:green:2014:7 | 3 | 36.525136 / 0.487590 / 37.132967 | 103.992781 / 0.702949 / 104.568593 | 184.72% | [177.49%, 188.84%] |
| pipeline_paired:pipeline:yellow:2011:1 | 3 | 64.564291 / 1.420336 / 65.164611 | 165.425588 / 11.027178 / 180.058090 | 156.22% | [155.85%, 178.52%] |
| pipeline_paired:pipeline:yellow:2011:10 | 3 | 70.303114 / 1.048584 / 70.340480 | 168.663407 / 6.012103 / 178.383042 | 139.91% | [138.03%, 162.96%] |
| pipeline_paired:pipeline:yellow:2011:4 | 3 | 68.302666 / 1.055773 / 69.028809 | 168.910391 / 5.216541 / 170.279451 | 147.30% | [131.51%, 154.38%] |
| pipeline_paired:pipeline:yellow:2011:7 | 3 | 67.768790 / 0.761058 / 68.535716 | 167.170753 / 0.808565 / 167.789760 | 146.68% | [144.62%, 147.76%] |
| service_cold_recorded:query:01_partition_financial_aggregation:green:2014:1 | 3 | 1.154303 / 0.019416 / 1.167601 | 6.737596 / 0.021272 / 6.755888 | 481.77% | [476.32%, 497.92%] |
| service_cold_recorded:query:01_partition_financial_aggregation:green:2014:10 | 3 | 1.122702 / 0.039225 / 1.188641 | 7.310498 / 0.129865 / 7.443143 | 541.15% | [523.59%, 554.17%] |
| service_cold_recorded:query:01_partition_financial_aggregation:green:2014:4 | 3 | 1.161521 / 0.039168 / 1.206201 | 7.587747 / 10.952681 / 27.185074 | 558.28% | [526.48%, 2427.94%] |
| service_cold_recorded:query:01_partition_financial_aggregation:green:2014:7 | 3 | 1.217556 / 0.023321 / 1.244243 | 7.014869 / 0.108127 / 7.084227 | 482.47% | [451.29%, 484.30%] |
| service_cold_recorded:query:01_partition_financial_aggregation:yellow:2011:1 | 3 | 1.432787 / 0.042394 / 1.439559 | 12.309436 / 0.922285 / 13.614961 | 779.03% | [759.13%, 855.35%] |
| service_cold_recorded:query:01_partition_financial_aggregation:yellow:2011:10 | 3 | 1.363098 / 0.039693 / 1.415254 | 13.312618 / 0.529884 / 14.250933 | 876.64% | [835.61%, 969.96%] |
| service_cold_recorded:query:01_partition_financial_aggregation:yellow:2011:4 | 3 | 1.463171 / 0.020835 / 1.492394 | 13.285118 / 0.448169 / 14.029551 | 808.95% | [788.26%, 864.50%] |
| service_cold_recorded:query:01_partition_financial_aggregation:yellow:2011:7 | 3 | 1.351271 / 0.035958 / 1.365924 | 13.244237 / 0.492709 / 13.854703 | 922.22% | [846.01%, 930.33%] |
| service_cold_recorded:query:02_pickup_location_aggregation:green:2014:1 | 3 | 1.195013 / 0.007442 / 1.197552 | 6.628383 / 0.097119 / 6.770078 | 454.67% | [450.29%, 473.64%] |
| service_cold_recorded:query:02_pickup_location_aggregation:green:2014:10 | 3 | 1.209590 / 0.020630 / 1.224167 | 7.378399 / 0.357785 / 7.679993 | 501.93% | [490.78%, 537.70%] |
| service_cold_recorded:query:02_pickup_location_aggregation:green:2014:4 | 3 | 1.263869 / 0.066748 / 1.272304 | 7.038336 / 0.111007 / 7.143622 | 456.89% | [444.54%, 527.80%] |
| service_cold_recorded:query:02_pickup_location_aggregation:green:2014:7 | 3 | 1.200311 / 0.046980 / 1.278171 | 6.775744 / 0.113534 / 6.912767 | 458.27% | [426.55%, 480.79%] |
| service_cold_recorded:query:02_pickup_location_aggregation:yellow:2011:1 | 3 | 1.453670 / 0.036821 / 1.474497 | 10.584534 / 0.242675 / 10.596659 | 628.13% | [584.76%, 655.29%] |
| service_cold_recorded:query:02_pickup_location_aggregation:yellow:2011:10 | 3 | 1.499708 / 0.029625 / 1.528487 | 10.167201 / 0.045461 / 10.248892 | 584.00% | [563.79%, 590.49%] |
| service_cold_recorded:query:02_pickup_location_aggregation:yellow:2011:4 | 3 | 1.502312 / 0.083011 / 1.631734 | 10.298159 / 0.237474 / 10.466500 | 566.32% | [525.60%, 608.42%] |
| service_cold_recorded:query:02_pickup_location_aggregation:yellow:2011:7 | 3 | 1.480167 / 0.036552 / 1.507645 | 10.569710 / 0.664712 / 11.305029 | 599.66% | [599.59%, 669.29%] |
| service_cold_recorded:query:03_dataset_financial_scan:green:2014:1 | 3 | 1.256901 / 0.007794 / 1.265806 | 7.858816 / 0.317182 / 8.165026 | 525.25% | [504.59%, 547.23%] |
| service_cold_recorded:query:03_dataset_financial_scan:yellow:2011:1 | 3 | 1.847069 / 0.052845 / 1.902287 | 23.937702 / 1.144053 / 24.235198 | 1154.32% | [1090.00%, 1246.19%] |
| warm_recorded:query:01_partition_financial_aggregation:green:2014:1 | 5 | 0.061786 / 0.005281 / 0.066901 | 1.457458 / 0.376543 / 2.826054 | 2349.21% | [1777.79%, 4610.04%] |
| warm_recorded:query:01_partition_financial_aggregation:green:2014:10 | 5 | 0.067795 / 0.001343 / 0.077458 | 3.681704 / 0.223190 / 3.994828 | 5294.97% | [4956.00%, 5568.79%] |
| warm_recorded:query:01_partition_financial_aggregation:green:2014:4 | 5 | 0.062909 / 0.010713 / 0.074658 | 3.565170 / 0.108605 / 3.700258 | 5549.15% | [4158.83%, 5658.36%] |
| warm_recorded:query:01_partition_financial_aggregation:green:2014:7 | 5 | 0.070991 / 0.006703 / 0.075229 | 3.329243 / 0.336373 / 3.686021 | 5074.98% | [2066.22%, 5503.33%] |
| warm_recorded:query:01_partition_financial_aggregation:yellow:2011:1 | 5 | 0.184947 / 0.011502 / 0.197042 | 9.426733 / 0.181490 / 10.167693 | 4969.28% | [4717.11%, 5479.84%] |
| warm_recorded:query:01_partition_financial_aggregation:yellow:2011:10 | 5 | 0.198601 / 0.005331 / 0.200262 | 9.444907 / 2.478532 / 11.501950 | 4607.44% | [4158.60%, 6126.00%] |
| warm_recorded:query:01_partition_financial_aggregation:yellow:2011:4 | 5 | 0.200446 / 0.001960 / 0.268331 | 9.179526 / 1.453465 / 10.128137 | 4455.31% | [2898.26%, 5007.59%] |
| warm_recorded:query:01_partition_financial_aggregation:yellow:2011:7 | 5 | 0.197654 / 0.000485 / 0.205745 | 9.290313 / 0.845318 / 9.537712 | 4600.29% | [4027.89%, 4808.11%] |
| warm_recorded:query:02_pickup_location_aggregation:green:2014:1 | 5 | 0.060994 / 0.004849 / 0.081222 | 2.957914 / 1.881046 / 3.189807 | 3369.76% | [1756.11%, 5234.33%] |
| warm_recorded:query:02_pickup_location_aggregation:green:2014:10 | 5 | 0.070935 / 0.005234 / 0.080697 | 3.333821 / 0.265418 / 4.137788 | 4685.16% | [3807.30%, 5588.81%] |
| warm_recorded:query:02_pickup_location_aggregation:green:2014:4 | 5 | 0.068262 / 0.006568 / 0.088123 | 3.104917 / 0.088920 / 3.368800 | 4448.56% | [3602.49%, 4907.34%] |
| warm_recorded:query:02_pickup_location_aggregation:green:2014:7 | 5 | 0.070074 / 0.004729 / 0.071432 | 2.940364 / 0.257742 / 3.327710 | 4341.04% | [1545.99%, 5147.64%] |
| warm_recorded:query:02_pickup_location_aggregation:yellow:2011:1 | 5 | 0.245930 / 0.008086 / 0.279438 | 6.108900 / 3.524774 / 17.442388 | 2511.23% | [1756.51%, 6696.16%] |
| warm_recorded:query:02_pickup_location_aggregation:yellow:2011:10 | 5 | 0.252204 / 0.013865 / 0.258963 | 6.179214 / 0.459854 / 6.754716 | 2350.09% | [1784.24%, 2765.56%] |
| warm_recorded:query:02_pickup_location_aggregation:yellow:2011:4 | 5 | 0.251827 / 0.022400 / 0.279171 | 5.835303 / 0.045309 / 6.079489 | 2223.55% | [1608.25%, 2393.68%] |
| warm_recorded:query:02_pickup_location_aggregation:yellow:2011:7 | 5 | 0.237087 / 0.005240 / 0.246379 | 6.371000 / 0.993343 / 8.309906 | 2472.82% | [2109.53%, 3586.86%] |
| warm_recorded:query:03_dataset_financial_scan:green:2014:1 | 5 | 0.069976 / 0.002715 / 0.075381 | 2.806409 / 1.477621 / 3.927176 | 3810.34% | [3002.19%, 5768.63%] |
| warm_recorded:query:03_dataset_financial_scan:yellow:2011:1 | 5 | 0.302296 / 0.035913 / 0.343258 | 14.945820 / 2.999294 / 20.572938 | 4748.54% | [4133.43%, 6521.88%] |

## Resource Proxies

| Architecture | CPU-seconds | Memory GiB-hours | Configured CPU-hours | Configured memory GiB-hours |
| --- | ---: | ---: | ---: | ---: |
| hybrid_aws | 3365.465500 | 6.718219 | 17.109540 | 51.328620 |
| onprem | 3860.777500 | 3.582100 | 7.941326 | 23.823979 |

## Aggregate AWS Cost Estimate

- Estimated marginal S3 cost: USD 0.97383823
- Scope: Aggregate S3 request and transfer estimate across evidence blocks.
- Cost Explorer reconciliation is intentionally deferred until daily service totals are available.

Quartiles and p95 use linear interpolation. Paired intervals use 10,000 deterministic bootstrap resamples with seed 20260713. No significance or p-value claim is made.

## Evidence Limitations

CloudWatch storage metrics are daily bucket/storage-class snapshots, not per-prefix or per-query evidence. S3 cost evidence is aggregate and must be reconciled against later daily Cost Explorer service totals. Local CPU and memory samples are resource proxies and are not translated to USD.

## Resilience Matrix

| Dimension | On-prem | Hybrid |
| --- | --- | --- |
| failure domain | single local site | local compute plus regional S3 and Glue |
| redundancy | operator-managed MinIO deployment | AWS-managed S3 and Glue service durability |
| recovery ownership | local operator | shared responsibility |
| backup/versioning | local policy | S3 versioning and lifecycle policy |
| monitoring | local container and service telemetry | local telemetry plus CloudWatch |
| encryption | local configuration | S3 and Glue configuration plus IAM |
| auditability | local logs | local logs plus AWS API audit controls |
| network dependency | LAN | internet or private AWS connectivity |
| service responsibility | operator owns all layers | AWS owns storage/catalog service infrastructure |

## Acceptance Gate

- All automated completeness and correctness gates passed.
- The user explicitly accepted this comparison on 2026-07-27.
- This comparison is the canonical Phase 3 baseline. Phase 4 may proceed under the definitive plan; evidence cleanup remains a separate, explicit action.
