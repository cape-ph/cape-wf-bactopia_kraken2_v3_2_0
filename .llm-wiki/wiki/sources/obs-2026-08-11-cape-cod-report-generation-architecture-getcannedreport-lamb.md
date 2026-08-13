---
type: source
title: "Observation: cape-cod report generation architecture (getcannedreport lambda, seqauto ETL/crawler/Athena chain)"
tags:
  - cape-cod
  - report
  - athena
  - glue
  - crawler
  - etl
  - mwaa
  - airflow
  - lambda
  - seqauto
status: observation
created: 2026-08-11
updated: 2026-08-11
slug: obs-2026-08-11-cape-cod-report-generation-architecture-getcannedreport-lamb
relevance: high
observed_at: 2026-08-11T18:35:28.485Z
source_context: Planning report generation into the bactopia/kraken2 workflow
---

# ⭐ Observation: cape-cod report generation architecture (getcannedreport lambda, seqauto ETL/crawler/Athena chain)

Investigated cape-cod report generation to plan moving it into the bactopia/kraken2 workflow DAG. Current report is on-demand via API Gateway -> getcannedreport lambda (assets/api/capi/handlers/get_canned_report.py): reads report metadata from cape-reports-CannedReportsStore DynamoDB by reportId (bactopia-single-sample-analysis), loads a Jinja template from the automation-assets bucket at reports/templates/<id>, synchronously invokes the data-function lambda (cape-reports-bctpssa-datfn, assets/report/bactopia-single-sample-analysis/data_function.py) with {sample_id}, renders HTML, optionally converts to PDF via weasyprint. For format=html the HTML is returned in response body (not base64). If the sample metadata is not yet catalogued the data function does meta_df.iloc[0] which raises, so the lambda returns 500 -> this 500-vs-200 is usable as a data-readiness probe. The data function runs Athena queries via awswrangler against the Glue catalog database whose name contains 'seqauto-catalog' (sample metadata + software_versions, sourmash gtdb species ID, amrfinderplus virulence/AMR, plus best-effort Caerbannog stoplight in a try/except that degrades to empty). Data flow: bactopia Batch writes to seqauto result-raw under pipeline-output/bactopia-runs/... -> S3 event -> src bucket trigger lambda -> SQS -> sqs_etl_job_trigger_lambda calls glue.start_job_run for bactopia-results/bactopia-samples ETL jobs (assets/etl/etl_bactopia_*.py) -> CSV to result-clean -> the result-clean Glue crawler (scheduled 0200 daily) catalogs result_* tables -> Athena queryable. ETL is fully async (one Glue job run per matching output file, run ids never handed to any orchestrator); newly written data is not queryable until a crawl runs. The result-clean crawler physical name is Pulumi-generated with a random suffix; crawler names are stored in a CrawlerAttrs DynamoDB table keyed by bucket and surfaced by the getbucketcrawler API. The MWAA (Airflow) execution role (capeinfra/pipeline/airflow.py) currently has AmazonS3FullAccess + AWSBatchFullAccess attached plus batch submit/pass-role and airflow:InvokeRestApi, but NO Glue and NO Athena permissions.

*Relevance: high*
*Context: Planning report generation into the bactopia/kraken2 workflow*
*Tags: cape-cod report athena glue crawler etl mwaa airflow lambda seqauto*

---
*Observed: 2026-08-11T18:35:28.485Z*
