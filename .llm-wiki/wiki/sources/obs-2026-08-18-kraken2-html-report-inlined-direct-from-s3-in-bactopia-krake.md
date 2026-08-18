---
type: source
title: "Observation: Kraken2 HTML report inlined direct-from-S3 in bactopia+kraken2 DAG"
tags:
  - cape
  - airflow
  - kraken2
  - report
  - s3
  - dag
status: observation
created: 2026-08-18
updated: 2026-08-18
slug: obs-2026-08-18-kraken2-html-report-inlined-direct-from-s3-in-bactopia-krake
relevance: high
observed_at: 2026-08-18T14:27:42.581Z
source_context: Inlining kraken2 taxonomic HTML report into the CAPE bactopia+kraken2 DAG
---

# ⭐ Observation: Kraken2 HTML report inlined direct-from-S3 in bactopia+kraken2 DAG

The kraken2 report in bactopia_kraken2_v3_2_0.py is no longer scaffolded/disabled and no longer uses the canned-report lambda. generate_and_store_kraken2_report is now enabled and wired via chain(submit_kraken2_job, generate_kraken2_report). submit_kraken2_job waits for completion (default), so the report file exists when the task runs. The task reads {outdir}/{sample}/tools/kraken2/{sample}.kraken2.report.txt from S3 (outdir + sample come from configs['bactopia']['nextflowOptions']), renders a self-contained HTML report, and writes it to the artifacts bucket at {REPORT_OUTPUT_PREFIX}/{sample}/kraken2.html (reports/<sample>/kraken2.html), next to bactopia.html. Silent-fail: if the report object is missing (NoSuchKey/404/NoSuchBucket via _read_s3_text_or_none) or empty, it logs and returns "" without raising, keeping the DAG green. A self-contained renderer (RANK_LABELS, parse_kraken2_report, summarize, top_species, build_tree, _default_open, _row_cells, _render_node, render_tree_html, KRAKEN2_REPORT_TEMPLATE, render_kraken2_report_html) is inlined as a clearly-delimited "KRAKEN2 REPORT (direct-from-S3 HTML)" section, deliberately isolated for later replacement by an official cape-cod report type. The HTML has summary cards, a top-species table, and a collapsible taxonomic tree with a 3-way Relevant/Expanded/Collapsed segmented control (each node with children is its own <details>; Relevant expands non-species branches >= DEFAULT_OPEN_PCT=1.0%, collapses species-level strain lists). No footer in the workflow report (user requested). Uses jinja2 Template (available in the Airflow env). A standalone dev/preview generator exists at scripts/kraken2_report.py (keeps a footer, has a CLI). Gotcha: ruff auto-fix strips import html and from jinja2 import Template while temporarily unused during multi-step edits; add the import in the same edit that introduces its use.

*Relevance: high*
*Context: Inlining kraken2 taxonomic HTML report into the CAPE bactopia+kraken2 DAG*
*Tags: cape airflow kraken2 report s3 dag*

---
*Observed: 2026-08-18T14:27:42.581Z*
