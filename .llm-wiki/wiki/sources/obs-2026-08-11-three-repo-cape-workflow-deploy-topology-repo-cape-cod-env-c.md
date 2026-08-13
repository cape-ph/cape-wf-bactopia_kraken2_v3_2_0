---
type: source
title: "Observation: Three-repo CAPE workflow deploy topology (repo -> cape-cod-env -> cape-cod)"
tags:
  - cape
  - cape-cod
  - cape-cod-env
  - deployment
  - airflow
  - pulumi
  - ansible
  - dynamodb
  - topology
status: observation
created: 2026-08-11
updated: 2026-08-11
slug: obs-2026-08-11-three-repo-cape-workflow-deploy-topology-repo-cape-cod-env-c
relevance: high
observed_at: 2026-08-11T17:57:21.756Z
source_context: Understanding cape-cod / cape-cod-env relationship to this workflow repo
---

# ⭐ Observation: Three-repo CAPE workflow deploy topology (repo -> cape-cod-env -> cape-cod)

Mapped the three-repo CAPE relationship for cape-wf-bactopia_kraken2_v3_2_0. This repo produces a release zip of exactly {dag.py, meta.json}. cape-cod-env (Ansible, cape_env_workflows role + library/deploy_workflows.py) fetches that release (remote HTTPS URL or local files/workflow_archives/), validates exactly-2-flat-files, uploads the DAG to s3://<bucket>/airflow/dags/<dag_id>.py and PutItem {dag_id, pipeline_ids} into the cape-workflow-meta-registry DynamoDB table. cape-cod (Pulumi IaC, capeinfra/pipeline/) defines the runtime: Airflow/MWAA reading airflow/dags, AWS Batch queues+job-definitions, and two DynamoDB registries - DAPRegistry (hash key pipeline_id, GSI pipeline_name+version, loaded from assets/analysis-pipelines DAP fixtures, carries parametersSchema the frontend uses) and WorkflowMetaRegistry (hash key dag_id; table created by cape-cod, entries written by cape-cod-env). Stable contracts: dag_id is the join key (S3 object name + Airflow id + workflow-meta hash key) which is why the v0.1.3 dag_id mismatch broke things; pipeline_ids must match registered pipeline_id entries. Workflow deploy is NOT idempotent. Captured as wiki pages syntheses/cape-system-topology, concepts/workflow-deployment-pipeline, entities/cape-cod, entities/cape-cod-env, kept general so sibling-repo internal changes don't invalidate them. Both siblings have their own .llm-wiki vaults and use the same `folder/page` wikilink convention. Note: CAPE = Center for Applied Pathogen Genomics (and Outbreak Control), corrected an earlier wrong acronym guess.

*Relevance: high*
*Context: Understanding cape-cod / cape-cod-env relationship to this workflow repo*
*Tags: cape cape-cod cape-cod-env deployment airflow pulumi ansible dynamodb topology*

---
*Observed: 2026-08-11T17:57:21.756Z*
