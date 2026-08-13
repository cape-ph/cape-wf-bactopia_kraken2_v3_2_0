---
type: source
title: "Observation: DAG report generation implemented (crawl-then-probe loop + getcannedreport html bytes caveat)"
tags:
  - bactopia
  - kraken2
  - dag
  - report
  - crawler
  - athena
  - lambda
  - mwaa
  - airflow
  - cape-cod
  - glue
status: observation
created: 2026-08-11
updated: 2026-08-11
slug: obs-2026-08-11-dag-report-generation-implemented-crawl-then-probe-loop-getc
relevance: high
observed_at: 2026-08-11T18:55:58.344Z
source_context: Implementing report generation into the bactopia/kraken2 workflow DAG
---

# ⭐ Observation: DAG report generation implemented (crawl-then-probe loop + getcannedreport html bytes caveat)

Implemented report generation in the bactopia_kraken2_v3_2_0 DAG (bactopia_kraken2_v3_2_0.py). Added a generate_and_store_report @task appended to chain() after submit_kraken2_job. It runs a bounded crawl-then-probe loop: run_crawler_and_wait starts the seqauto result-clean Glue crawler (RESULT_CLEAN_CRAWLER_NAME=ccd-dlh-T-seqauto-result-clean-vbkt-crwl-gcrwl-1ceb6f5) and polls get_crawler until State==READY; invoke_report_lambda invokes the getcannedreport lambda (arn ...ccd-pvsl-capi-api-getcannedreport-lmbdfn-b295d26, us-east-2) with queryStringParameters reportId/sampleId/format=html; on statusCode 200 it writes body to s3://cape-demo-files/artifacts/<sample>/bactopia.html, else sleeps and re-crawls (only a crawl surfaces newly ETL'd clean data). sample_id read via ti.xcom_pull from validate_and_extract_nextflow_configs (avoids XComArg __getitem__ typing error under pyright; pass **context, not the XComArg). boto3 is available in MWAA; no new deps. All constants hardcoded for the demo with TODOs. IMPORTANT caveat: for format=html the deployed get_canned_report.py handler sets response body to bytes (io.BytesIO(...).getvalue()), which API Gateway rejects and the Lambda runtime json.dumps cannot serialize, so the html path is likely broken today (git history shows only the PDF/base64 path was validated - commit 66525ad). Must verify by invoking the deployed lambda before relying on it; if broken, a one-line cape-cod fix returns report_html (str) as body. cape-cod still needs MWAA execution role perms added: glue:StartCrawler, glue:GetCrawler, lambda:InvokeFunction (S3 write already covered by attached AmazonS3FullAccess). Hard precondition: bactopia --outdir must route to the seqauto result-raw bucket under pipeline-output/bactopia-runs so the ETL/crawler/Athena chain fires. DAG validated: ruff clean, pi-lens LSP clean, imports under Airflow 3.0.6. Plan captured in PLAN.md.

*Relevance: high*
*Context: Implementing report generation into the bactopia/kraken2 workflow DAG*
*Tags: bactopia kraken2 dag report crawler athena lambda mwaa airflow cape-cod glue*

---
*Observed: 2026-08-11T18:55:58.344Z*
