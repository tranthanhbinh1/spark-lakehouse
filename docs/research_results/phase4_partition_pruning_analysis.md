# Phase 4 Partition-Pruning Analysis

- Source comparison: `phase3_baseline_20260727T035807Z_fde426a`
- Source protocols: `warm_recorded`, `service_cold_recorded`
- Status: **AUTOMATED ANALYSIS PASSED**

## Result

Partition filters reduced physical input in every measured case, but they did not consistently narrow the relative hybrid-versus-on-prem latency penalty. This supports pruning as an absolute I/O mitigation, not as a complete explanation or removal of hybrid overhead.

## Aggregate Findings

- Physical-input reduction: 69.44% to 83.67%.
- Median-latency improvement: 28/32 architecture/protocol/partition cases.
- Hybrid median-latency improvement: 13/16 protocol/partition cases.
- Relative hybrid penalty narrowed: 10/16 protocol/partition cases.

## Physical Input And Latency

| Protocol | Architecture | Partition | n | Broad median (s) | Filtered median (s) | Latency reduction | Broad bytes | Filtered bytes | Input reduction |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| warm_recorded | onprem | yellow 2011-01 | 5 | 0.302296 | 0.184947 | 38.82% | 241153632 | 55874946 | 76.83% |
| warm_recorded | onprem | yellow 2011-04 | 5 | 0.302296 | 0.200446 | 33.69% | 241153632 | 60993045 | 74.71% |
| warm_recorded | onprem | yellow 2011-07 | 5 | 0.302296 | 0.197654 | 34.62% | 241153632 | 59804135 | 75.20% |
| warm_recorded | onprem | yellow 2011-10 | 5 | 0.302296 | 0.198601 | 34.30% | 241153632 | 64481506 | 73.26% |
| warm_recorded | onprem | green 2014-01 | 5 | 0.069976 | 0.061786 | 11.70% | 16850101 | 2751421 | 83.67% |
| warm_recorded | onprem | green 2014-04 | 5 | 0.069976 | 0.062909 | 10.10% | 16850101 | 4533446 | 73.10% |
| warm_recorded | onprem | green 2014-07 | 5 | 0.069976 | 0.070991 | -1.45% | 16850101 | 4415673 | 73.79% |
| warm_recorded | onprem | green 2014-10 | 5 | 0.069976 | 0.067795 | 3.12% | 16850101 | 5149561 | 69.44% |
| warm_recorded | hybrid_aws | yellow 2011-01 | 5 | 14.945820 | 9.426733 | 36.93% | 241153632 | 55874946 | 76.83% |
| warm_recorded | hybrid_aws | yellow 2011-04 | 5 | 14.945820 | 9.179526 | 38.58% | 241153632 | 60993045 | 74.71% |
| warm_recorded | hybrid_aws | yellow 2011-07 | 5 | 14.945820 | 9.290313 | 37.84% | 241153632 | 59804135 | 75.20% |
| warm_recorded | hybrid_aws | yellow 2011-10 | 5 | 14.945820 | 9.444907 | 36.81% | 241153632 | 64481506 | 73.26% |
| warm_recorded | hybrid_aws | green 2014-01 | 5 | 2.806409 | 1.457458 | 48.07% | 16850101 | 2751421 | 83.67% |
| warm_recorded | hybrid_aws | green 2014-04 | 5 | 2.806409 | 3.565170 | -27.04% | 16850101 | 4533446 | 73.10% |
| warm_recorded | hybrid_aws | green 2014-07 | 5 | 2.806409 | 3.329243 | -18.63% | 16850101 | 4415673 | 73.79% |
| warm_recorded | hybrid_aws | green 2014-10 | 5 | 2.806409 | 3.681704 | -31.19% | 16850101 | 5149561 | 69.44% |
| service_cold_recorded | onprem | yellow 2011-01 | 3 | 1.847069 | 1.432787 | 22.43% | 241153632 | 55874946 | 76.83% |
| service_cold_recorded | onprem | yellow 2011-04 | 3 | 1.847069 | 1.463171 | 20.78% | 241153632 | 60993045 | 74.71% |
| service_cold_recorded | onprem | yellow 2011-07 | 3 | 1.847069 | 1.351271 | 26.84% | 241153632 | 59804135 | 75.20% |
| service_cold_recorded | onprem | yellow 2011-10 | 3 | 1.847069 | 1.363098 | 26.20% | 241153632 | 64481506 | 73.26% |
| service_cold_recorded | onprem | green 2014-01 | 3 | 1.256901 | 1.154303 | 8.16% | 16850101 | 2751421 | 83.67% |
| service_cold_recorded | onprem | green 2014-04 | 3 | 1.256901 | 1.161521 | 7.59% | 16850101 | 4533446 | 73.10% |
| service_cold_recorded | onprem | green 2014-07 | 3 | 1.256901 | 1.217556 | 3.13% | 16850101 | 4415673 | 73.79% |
| service_cold_recorded | onprem | green 2014-10 | 3 | 1.256901 | 1.122702 | 10.68% | 16850101 | 5149561 | 69.44% |
| service_cold_recorded | hybrid_aws | yellow 2011-01 | 3 | 23.937702 | 12.309436 | 48.58% | 241153632 | 55874946 | 76.83% |
| service_cold_recorded | hybrid_aws | yellow 2011-04 | 3 | 23.937702 | 13.285118 | 44.50% | 241153632 | 60993045 | 74.71% |
| service_cold_recorded | hybrid_aws | yellow 2011-07 | 3 | 23.937702 | 13.244237 | 44.67% | 241153632 | 59804135 | 75.20% |
| service_cold_recorded | hybrid_aws | yellow 2011-10 | 3 | 23.937702 | 13.312618 | 44.39% | 241153632 | 64481506 | 73.26% |
| service_cold_recorded | hybrid_aws | green 2014-01 | 3 | 7.858816 | 6.737596 | 14.27% | 16850101 | 2751421 | 83.67% |
| service_cold_recorded | hybrid_aws | green 2014-04 | 3 | 7.858816 | 7.587747 | 3.45% | 16850101 | 4533446 | 73.10% |
| service_cold_recorded | hybrid_aws | green 2014-07 | 3 | 7.858816 | 7.014869 | 10.74% | 16850101 | 4415673 | 73.79% |
| service_cold_recorded | hybrid_aws | green 2014-10 | 3 | 7.858816 | 7.310498 | 6.98% | 16850101 | 5149561 | 69.44% |

## Relative Hybrid Penalty

Positive narrowing means the filtered query reduced the relative hybrid penalty. Negative narrowing means the relative penalty grew.

| Protocol | Partition | Broad hybrid penalty | Filtered hybrid penalty | Narrowing |
| --- | --- | ---: | ---: | ---: |
| warm_recorded | yellow 2011-01 | 4844.11% | 4997.00% | -152.89 pp |
| warm_recorded | yellow 2011-04 | 4844.11% | 4479.56% | 364.55 pp |
| warm_recorded | yellow 2011-07 | 4844.11% | 4600.29% | 243.82 pp |
| warm_recorded | yellow 2011-10 | 4844.11% | 4655.72% | 188.39 pp |
| warm_recorded | green 2014-01 | 3910.53% | 2258.88% | 1651.65 pp |
| warm_recorded | green 2014-04 | 3910.53% | 5567.18% | -1656.65 pp |
| warm_recorded | green 2014-07 | 3910.53% | 4589.65% | -679.12 pp |
| warm_recorded | green 2014-10 | 3910.53% | 5330.68% | -1420.15 pp |
| service_cold_recorded | yellow 2011-01 | 1195.98% | 759.13% | 436.86 pp |
| service_cold_recorded | yellow 2011-04 | 1195.98% | 807.97% | 388.02 pp |
| service_cold_recorded | yellow 2011-07 | 1195.98% | 880.13% | 315.85 pp |
| service_cold_recorded | yellow 2011-10 | 1195.98% | 876.64% | 319.34 pp |
| service_cold_recorded | green 2014-01 | 525.25% | 483.69% | 41.56 pp |
| service_cold_recorded | green 2014-04 | 525.25% | 553.26% | -28.01 pp |
| service_cold_recorded | green 2014-07 | 525.25% | 476.14% | 49.11 pp |
| service_cold_recorded | green 2014-10 | 525.25% | 551.15% | -25.90 pp |

## Interpretation Limits

- Broad scans aggregate four measured monthly partitions; filtered queries aggregate one month. This is a pruning workload comparison, not an identical-result comparison.
- Each dataset-wide broad-scan median is reused as the reference for its four monthly filters. The 32 rows are comparison cases, not 32 independent broad-scan samples.
- Warm and service-cold samples are analyzed separately. No p-value or causal significance claim is made.
- Identical physical-input byte counts across architectures are consistent with matched table contents and observed file counts; they do not prove identical object-store internals.

## Phase 4 Decision

Partition pruning is retained as a supported practical mitigation for absolute I/O and improved hybrid median latency in most cases. It is not sufficient by itself to eliminate the hybrid latency penalty. H2 is partially supported for query-layout optimization; request-count reduction was not directly measured.
