---
type: source
title: "Observation: DAG config format reworked to dag_run.conf/pipelineConfigs (PR #11); wiki synced"
tags:
  - cape
  - airflow
  - dag
  - config-format
  - xcom
  - taskflow
  - sync
  - pyright
status: observation
created: 2026-08-11
updated: 2026-08-11
slug: obs-2026-08-11-dag-config-format-reworked-to-dag-run-conf-pipelineconfigs-p
relevance: high
observed_at: 2026-08-11T18:05:35.197Z
source_context: Syncing wiki to pulled commits (config-format rework)
---

# ⭐ Observation: DAG config format reworked to dag_run.conf/pipelineConfigs (PR #11); wiki synced

Synced the llm-wiki to commit c02762d (PR #11, issue #9 config-format rework) of cape-wf-bactopia_kraken2_v3_2_0. The DAG bactopia_kraken2_v3_2_0.py was substantially rewritten: config now arrives via dag_run.conf as {pipelineConfigs: [{pipelineId, nextflowOptions}]} instead of templated params.bactopia.*/params.kraken2.*. A new TaskFlow @task validate_and_extract_nextflow_configs validates against an EXPECTED_PIPELINES constant (bactopia requires --sample/--outdir and accepts pipelineId bactopia-ont-v3.2.0; kraken2 no required fields, accepts bactopia-kraken2-v3.2.0), flattens nextflowOptions to a CLI string, and passes results downstream via XCom. Output S3 bucket is now derived from bactopia --outdir via a user_defined_filters Jinja filter extract_s3_bucket (helper extract_s3_bucket_name). Kraken2 flags (--wf/--bactopia/--kraken2_db) now come from user nextflowOptions, not hard-coded. Helpers are written for future extraction into a shared DAG library. Imports were trimmed (removed compute-env/ECS/batch-sensor/boto3/io). pyright ^1.1.410 added as a Poetry dev dependency (in poetry.lock) though not yet installed in local .venv. meta.json unchanged. README.md still documents the OLD params format and is now stale. Updated wiki pages: metajson-and-parameters, workflow-task-flow, bactopia-kraken2-dag, features-and-entry-points, architecture-overview, coding-style-and-conventions, dev-environment-and-tooling. Issue #10 will remove hard-coded PROJ/VERSION constants by sourcing from CAPE /workflows/pipelineprofiles endpoint.

*Relevance: high*
*Context: Syncing wiki to pulled commits (config-format rework)*
*Tags: cape airflow dag config-format xcom taskflow sync pyright*

---
*Observed: 2026-08-11T18:05:35.197Z*
