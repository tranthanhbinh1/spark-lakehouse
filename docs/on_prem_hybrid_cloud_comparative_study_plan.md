# On-Premises vs Hybrid Cloud Data Workload Comparative Study Plan

## Purpose

This study compares the current on-premises lakehouse MVP against one or more hybrid cloud variants for batch analytical data workloads.

The immediate goal is not to implement cloud architecture yet. The next phase is to make the existing MVP study-ready: reproducible, measurable, and instrumented enough that later cloud comparisons are defensible.

## Research Question

For batch analytical data workloads, how do on-premises and hybrid cloud lakehouse architectures compare on performance, cost, operability, reliability, security, and portability?

## Baseline Definitions

Use the NIST cloud model as the terminology baseline: cloud systems provide network-accessible pooled resources that can be rapidly provisioned, released, and measured. NIST also distinguishes deployment models such as private, public, community, and hybrid cloud.

Reference: [NIST SP 800-145, The NIST Definition of Cloud Computing](https://www.nist.gov/publications/nist-definition-cloud-computing)

## Architectures To Compare

### 1. On-Premises Baseline

The existing MVP stack:

- Airflow for orchestration
- Spark for batch processing
- Iceberg for table format
- MinIO-compatible S3A object storage
- Trino for SQL analytics
- Local infrastructure and self-managed services

This becomes the control architecture.

### 2. Hybrid Storage

Compute and orchestration remain local, while one or more storage or metadata services move to cloud infrastructure.

Possible variants:

- Local Spark writes to cloud object storage
- Local Trino queries Iceberg tables backed by cloud object storage
- Cloud-hosted catalog with local compute

This isolates the effect of remote storage, network latency, object-store behavior, and egress cost.

### 3. Hybrid Compute Burst

Storage and table metadata remain shared, while selected compute workloads can run in cloud infrastructure.

Possible variants:

- Spark batch job runs in cloud against shared Iceberg data
- Trino or another query engine runs in cloud against the same tables
- Airflow remains local and submits remote workloads

This isolates elasticity, provisioning overhead, runtime performance, and operational complexity.

### 4. Optional Cloud Reference Architecture

A fully cloud-hosted version is useful as a reference point, but it should not be confused with hybrid architecture.

This can answer: "How much operational simplicity or elasticity do we gain if we stop preserving on-premises control?"

## Study Validity Constraints

Avoid these invalid comparisons:

- Comparing a laptop Docker stack to managed cloud services without normalizing hardware and service levels
- Changing storage, compute, catalog, orchestration, and networking all at once
- Measuring only a single run
- Ignoring cache effects
- Treating cloud bill as cost while treating on-premises infrastructure as free
- Ignoring data egress in hybrid designs
- Using screenshots or manual observations as primary evidence

## Workloads

Use the NYC TLC pipeline as the benchmark workload family.

### Batch Ingestion Workloads

- Small: one month
- Medium: one year
- Large: all available years
- Incremental rerun: overwrite one existing month partition
- Mixed dataset run: yellow and green taxi datasets

### Query Workloads

Create a fixed Trino SQL query suite over the Iceberg silver tables.

Suggested query groups:

- Row counts by year and month
- Revenue aggregation by period
- Pickup/dropoff location aggregation
- Long-running scan-heavy query
- Selective partition-filtered query
- Join or multi-table analytical query if gold tables are added

### Failure And Recovery Workloads

- Failed Spark job retry
- Re-run of an already written month
- Missing input file behavior
- Catalog unavailable behavior
- Object store unavailable behavior
- Airflow task retry and recovery timing

## Metrics

### Performance

- Spark job wall-clock runtime
- Spark stage/task duration
- Input bytes read
- Output bytes written
- Number and size of output files
- Trino query latency
- Cold-cache vs warm-cache query latency
- Partition overwrite duration

### Cost

- Compute runtime
- Storage used
- Object storage API requests
- Network transfer
- Data egress
- Estimated on-premises hardware amortization
- Estimated power usage
- Operator time for maintenance and recovery

### Operability

- Deployment steps
- Manual actions required
- Configuration files and environment variables required
- Secret-management complexity
- Mean time to diagnose common failures
- Mean time to recover failed workloads

### Reliability

- Idempotent partition rerun behavior
- Airflow retry correctness
- Iceberg table consistency after failed writes
- Catalog recovery behavior
- Object-store recovery behavior
- Data correctness after repeated runs

### Security And Governance

- Identity and access model
- Secret exposure risk
- Network exposure
- Encryption at rest
- Encryption in transit
- Audit log availability
- Least-privilege feasibility

### Portability

- Code changes required per architecture
- Config changes required per architecture
- Compatibility of Iceberg metadata
- Compatibility of Spark and Trino clients
- Vendor-specific services introduced

## Measurement Plan Before Cloud Implementation

### 1. Create Benchmark Assets

Add a benchmark directory with:

- Workload definitions
- Trino SQL query files
- Expected datasets and partition ranges
- Run manifests
- Result schema documentation

Suggested path:

```text
benchmarks/
  README.md
  workloads/
  queries/
  manifests/
```

### 2. Add Environment Profiles

Define environment profiles before implementing cloud variants.

Suggested path:

```text
conf/environments/
  onprem.toml
  hybrid-storage.toml
  hybrid-compute.toml
```

Each profile should describe:

- Object storage endpoint
- Catalog endpoint
- Spark master or submit mode
- Trino endpoint
- Airflow deployment target
- Secret source
- Network assumptions

### 3. Add Metrics Storage

Create an Iceberg-backed benchmark metrics table.

Suggested table:

```text
lakehouse.benchmark.run_metrics
```

Suggested fields:

- `run_id`
- `architecture`
- `environment`
- `workload_name`
- `dataset`
- `year`
- `month`
- `started_at`
- `finished_at`
- `duration_seconds`
- `status`
- `input_bytes`
- `output_bytes`
- `records_read`
- `records_written`
- `error_class`
- `error_message`
- `git_sha`
- `config_hash`

### 4. Capture Engine-Specific Metrics

Spark:

- Enable and preserve Spark event logs
- Capture application ID
- Capture stage and task timing
- Capture input and output bytes

Airflow:

- Capture DAG run ID
- Capture task duration
- Capture retry count
- Capture failure reason

Trino:

- Capture query ID
- Capture elapsed time
- Capture queued time
- Capture input rows and bytes
- Capture output rows

Object storage:

- Capture bucket size before and after runs
- Capture object count
- Capture request counts where available
- Capture transfer volume

### 5. Run Repeated Baseline Experiments

Before any cloud work, run the on-premises baseline repeatedly.

Minimum repetitions:

- 3 runs for each ingestion workload
- 5 runs for each query workload
- Separate cold-cache and warm-cache query measurements

Record environment state:

- Host CPU and memory
- Docker container resource limits
- Disk type and available space
- Spark worker count
- Spark executor configuration
- Trino worker count
- Network assumptions

## Evaluation Framework

Use a weighted scorecard informed by well-architected architecture review categories: operational excellence, security, reliability, performance efficiency, cost optimization, and sustainability.

References:

- [AWS Well-Architected Framework](https://docs.aws.amazon.com/wellarchitected/latest/framework/welcome.html)
- [Google Cloud Architecture Framework](https://docs.cloud.google.com/architecture/framework)

Suggested scorecard:

| Dimension | Weight | Measurement Type |
| --- | ---: | --- |
| Performance | 25% | Quantitative |
| Cost | 20% | Quantitative + estimate |
| Operability | 20% | Quantitative + qualitative rubric |
| Reliability | 15% | Quantitative failure tests |
| Security/Governance | 10% | Qualitative rubric |
| Portability | 10% | Code/config delta |

Weights should be adjusted only after the study objective is finalized.

## Recommended Phase Plan

### Phase 1: Make The MVP Measurable

- Add benchmark workload definitions
- Add query suite
- Add metrics table
- Add run IDs and config hashes
- Capture Spark, Airflow, Trino, and object-store metrics
- Produce a repeatable on-premises baseline report

### Phase 2: Harden The On-Premises Baseline

- Verify idempotent reruns
- Verify partition overwrite behavior
- Add failure and recovery tests
- Validate data correctness after retries
- Document operational runbooks
- Document current security posture

### Phase 3: Design Hybrid Variants

- Choose which layer moves first: storage, catalog, or compute
- Define expected tradeoffs before implementation
- Define success criteria for each variant
- Create infrastructure-as-code plan
- Estimate cost and egress exposure before running workloads

### Phase 4: Implement One Hybrid Variant At A Time

Recommended order:

1. Hybrid storage
2. Hybrid catalog
3. Hybrid compute burst
4. Optional fully cloud-hosted reference

Do not implement multiple variants simultaneously. That destroys attribution.

### Phase 5: Compare And Report

For each architecture:

- Run identical workloads
- Run repeated measurements
- Normalize results
- Compare cost/performance
- Compare operational complexity
- Compare failure recovery
- Document tradeoffs and assumptions

## Expected Deliverables

- Benchmark harness
- SQL query suite
- Metrics schema
- On-premises baseline report
- Hybrid architecture design document
- Infrastructure-as-code for hybrid variants
- Comparative results report
- Final recommendation matrix

## Immediate Next Step

Turn the current MVP into a reproducible benchmark harness before adding cloud infrastructure.

Without this step, later cloud comparisons will be weak because there will be no trustworthy baseline.

