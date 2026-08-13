---
type: source
title: "Observation: Verified getcannedreport html endpoint returns 200 + HTML string (cape-cod fix deployed)"
tags:
  - bactopia
  - report
  - getcannedreport
  - lambda
  - api-gateway
  - cape-cod
  - verification
  - mwaa
  - iam
status: observation
created: 2026-08-12
updated: 2026-08-12
slug: obs-2026-08-12-verified-getcannedreport-html-endpoint-returns-200-html-stri
relevance: high
observed_at: 2026-08-12T18:30:14.658Z
source_context: Verifying deployed getcannedreport html endpoint before finalizing DAG report step
---

# ⭐ Observation: Verified getcannedreport html endpoint returns 200 + HTML string (cape-cod fix deployed)

Verified the cape-cod getcannedreport html fix is deployed and working. GET https://api.cape-dev.org/capi-dev/report/create?format=html&reportId=bactopia-single-sample-analysis&sampleId=abcdefghij returns HTTP 200, Content-Type text/html, 31786 bytes of valid rendered HTML (CAPE Bactopia Single Sample Analysis Report). This supersedes the earlier caveat that the html path returned bytes and was broken - the user updated cape-cod and it now returns a proper string body. Note the dev API uses a self-signed cert chain, so curl needs -k. The DAG (bactopia_kraken2_v3_2_0.py) invokes the lambda directly via boto3 lambda.invoke (not through this API Gateway path), but the handler returns the same proxy dict {statusCode, headers, body} that invoke_report_lambda parses, so behavior matches; query keys reportId/sampleId/format align. Remaining cross-repo work: add glue:StartCrawler, glue:GetCrawler, lambda:InvokeFunction to the MWAA execution role in cape-cod (S3 write already covered by attached AmazonS3FullAccess).

*Relevance: high*
*Context: Verifying deployed getcannedreport html endpoint before finalizing DAG report step*
*Tags: bactopia report getcannedreport lambda api-gateway cape-cod verification mwaa iam*

---
*Observed: 2026-08-12T18:30:14.658Z*
