---
type: source
title: "Observation: kraken2 profile report scaffolded (disabled) in bactopia+kraken2 DAG"
tags:
  - airflow
  - cape
  - kraken2
  - report
  - scaffold
  - s3
status: observation
created: 2026-08-17
updated: 2026-08-17
slug: obs-2026-08-17-kraken2-profile-report-scaffolded-disabled-in-bactopia-krake
relevance: high
observed_at: 2026-08-17T17:19:33.968Z
source_context: Scaffolding a second (kraken2) report path in the CAPE Airflow workflow
---

# ⭐ Observation: kraken2 profile report scaffolded (disabled) in bactopia+kraken2 DAG

Scaffolded a kraken2 profile report in cape-wf-bactopia_kraken2_v3_2_0/bactopia_kraken2_v3_2_0.py, mirroring the bactopia report path. Refactored: extracted private helper _generate_and_store_report(sample_id, report_id, output_filename) containing the crawl-then-probe loop + S3 put; generate_and_store_report is now a thin wrapper using REPORT_ID/REPORT_OUTPUT_FILENAME. invoke_report_lambda gained report_id param defaulting to REPORT_ID (backward compatible). New constants KRAKEN2_REPORT_ID='kraken2-single-sample-analysis' (placeholder, TODO confirm real deployed reportId) and KRAKEN2_REPORT_OUTPUT_FILENAME='kraken2.html' (same bucket/prefix, per-sample). New @task generate_and_store_kraken2_report is DEFINED BUT NOT WIRED into the DAG - a commented SCAFFOLD enablement block at the end of bactopia_and_kraken2_v3_2_0() shows the 2 lines to uncomment (generate_kraken2_report = generate_and_store_kraken2_report(); chain(submit_kraken2_job, generate_kraken2_report)). Left disabled intentionally so the parallelization change can be tested against an unchanged DAG first. kraken2 report reports on the same bactopia --sample value.

*Relevance: high*
*Context: Scaffolding a second (kraken2) report path in the CAPE Airflow workflow*
*Tags: airflow cape kraken2 report scaffold s3*

---
*Observed: 2026-08-17T17:19:33.968Z*
