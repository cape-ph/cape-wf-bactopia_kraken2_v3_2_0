---
type: source
title: "Observation: bactopia+kraken2 DAG parallelized via QC-output S3 gate"
tags:
  - airflow
  - cape
  - bactopia
  - kraken2
  - dag
  - parallelism
status: observation
created: 2026-08-17
updated: 2026-08-17
slug: obs-2026-08-17-bactopia-kraken2-dag-parallelized-via-qc-output-s3-gate
relevance: high
observed_at: 2026-08-17T17:06:54.088Z
source_context: Parallelizing bactopia and kraken2 in the CAPE Airflow workflow
---

# ⭐ Observation: bactopia+kraken2 DAG parallelized via QC-output S3 gate

In cape-wf-bactopia_kraken2_v3_2_0/bactopia_kraken2_v3_2_0.py the DAG was changed from sequential (bactopia BatchOperator blocks to completion, then kraken2) to parallel. submit_bactopia_batch_job now sets wait_for_completion=False so it submits and returns immediately. A new S3KeySensor task 'wait_for_bactopia_qc_output' (mode=reschedule, full s3:// url in bucket_key, no bucket_name) gates kraken2 on the per-sample QC file at <outdir>/pipeline-output/<sample>/main/qc/<sample>.fastq.gz. Kraken2 runs while bactopia is still executing. A new BatchSensor 'wait_for_bactopia_complete' (job_id via Jinja xcom_pull on submit_bactopia_batch_job, mode=reschedule) waits for the full bactopia job before generate_report. create_k2_include/wait_for_kraken_2_include_file were decoupled from bactopia and now depend only on configs. Dependencies expressed with chain() (not >>) to avoid pyright reportUnusedExpression. Note: BatchSensor.job_id is typed str, so submit_bactopia_job.output (XComArg) fails pyright; use a templated string instead.

*Relevance: high*
*Context: Parallelizing bactopia and kraken2 in the CAPE Airflow workflow*
*Tags: airflow cape bactopia kraken2 dag parallelism*

---
*Observed: 2026-08-17T17:06:54.088Z*
