---
type: source
title: "Observation: Report readiness needs both input-clean and result-clean crawlers (input_meta + results)"
tags:
  - bactopia
  - report
  - crawler
  - input-clean
  - result-clean
  - glue
  - athena
  - seqauto
  - dag
  - mwaa
status: observation
created: 2026-08-13
updated: 2026-08-13
slug: obs-2026-08-13-report-readiness-needs-both-input-clean-and-result-clean-cra
relevance: high
observed_at: 2026-08-13T18:01:41.708Z
source_context: Fixing report readiness to also crawl the seqauto input-clean crawler
---

# ⭐ Observation: Report readiness needs both input-clean and result-clean crawlers (input_meta + results)

The bactopia report readiness in the bactopia_kraken2_v3_2_0 DAG requires crawling TWO seqauto crawlers, not just result-clean. The getcannedreport data function reads sample metadata from the input-clean catalog (input_meta table) and bactopia results from the result-clean catalog, so both must be crawled/caught-up before the report is queryable. Updated generate_and_store_report to loop over SEQAUTO_CRAWLER_NAMES = (INPUT_CLEAN_CRAWLER_NAME, RESULT_CLEAN_CRAWLER_NAME), input-clean first so input_meta is catalogued before the result tables that reference it. Physical names (Pulumi-generated, us-east-2): input-clean = ccd-dlh-T-seqauto-input-clean-vbkt-crwl-gcrwl-be77632; result-clean = ccd-dlh-T-seqauto-result-clean-vbkt-crwl-gcrwl-1ceb6f5. No cape-cod IAM change needed: configure_report_generation_perms in capeinfra/swimlanes/private.py already grants glue:StartCrawler/GetCrawler on every tributary crawler (crawler_arns spans all tributaries), so input-clean is already covered. TODO in the DAG: source both crawler names from the CrawlerAttrs DynamoDB table via the getbucketcrawler API rather than hard coding.

*Relevance: high*
*Context: Fixing report readiness to also crawl the seqauto input-clean crawler*
*Tags: bactopia report crawler input-clean result-clean glue athena seqauto dag mwaa*

---
*Observed: 2026-08-13T18:01:41.708Z*
