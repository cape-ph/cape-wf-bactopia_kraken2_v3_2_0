---
type: concept
title: Workflow Deployment Pipeline
created: 2026-08-11
updated: 2026-08-11
---

# Workflow Deployment Pipeline

How the release artifact from this repo actually reaches the running CAPE environment. The deploy mechanism lives in **cape-cod-env** ([[entities/cape-cod-env]]); this page captures the contract as seen from here, so it stays useful even as that repo changes internally.

## Trigger

An operator runs the cape-cod-env Ansible deploy (the `cape_env_workflows` role) against an inventory. Workflow deployment is one part of the broader environment deploy (which also runs DB migrations and resource ingestion).

## Sources of archives

- **Remote**: HTTPS release URLs listed in group vars (e.g. this repo's GitHub release zip). Restricted to HTTPS and an optional domain allowlist.
- **Local**: zip files checked into the role's `files/workflow_archives/` directory.

Both feed the same validator and deploy step. A test copy of this workflow's archive currently exists in cape-cod-env's local archives.

## Validation contract

Each archive must be a valid zip containing **exactly two flat files**: one `.py` (the DAG) and one `meta.json`. `meta.json` must contain non-empty `dag_id` and `pipeline_ids`. Anything else is logged and skipped (best-effort, non-fatal).

## What deploy does per archive

1. Extract the zip.
2. Read `meta.json`.
3. Upload the DAG bytes to S3 at `<dags_prefix>/<dag_id>.py` (prefix is `airflow/dags` by default) in the workflow bucket. Airflow picks the DAG up from there.
4. `PutItem` `{dag_id, pipeline_ids}` into the workflow-meta-registry DynamoDB table.

The S3 object name and the DynamoDB hash key are both derived from `meta.json`'s `dag_id` - see the join-key note in [[syntheses/cape-system-topology]].

## Known limitations (documented in cape-cod-env, relevant to us)

- **Not idempotent**: every run redeploys every workflow regardless of change. Intended future fix keys off release version numbers.
- **Scant error handling** in the deploy script; failures are logged, not raised (except temp-dir creation).

## Implications for work in this repo

- Publishing a release is the delivery mechanism (see [[concepts/release-and-distribution]]); nothing here is deployed until cape-cod-env picks up the archive.
- Keep the archive to exactly the two files and keep `dag_id`/`pipeline_ids` correct, or the deploy silently skips or misregisters the workflow.
