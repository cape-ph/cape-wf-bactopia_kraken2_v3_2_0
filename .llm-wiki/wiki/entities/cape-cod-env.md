---
type: entity
title: cape-cod-env
created: 2026-08-11
updated: 2026-08-11
---

# cape-cod-env

**External, separately-maintained repository.** The Ansible deployment repo that puts workflows (and DB migrations, resource ingestion) onto the CAPE infrastructure built by [[entities/cape-cod]]. Described here by role and contract; its internal roles/vars may change independently.

## Role

- Ansible (ansible-core 2.20.x, Python 3.12) repo. All tasks run locally against AWS via boto3; no remote hosts.
- Full deploy (`cape-cod-env.yaml`) runs DB migrations first (`cape-cod-db` Alembic package), then workflow deployment, then resource ingestion.

## Workflow deployment (the part that consumes this repo)

- The `cape_env_workflows` role + its custom module `deploy_workflows.py` deploy workflow archives.
- Archives come from **remote** HTTPS release URLs (configured in group vars; e.g. this repo's GitHub release) and/or **local** zips in the role's `files/workflow_archives/`.
- For each valid archive it uploads `<dag_id>.py` to `s3://<bucket>/airflow/dags/` and writes `{dag_id, pipeline_ids}` to the workflow-meta-registry DynamoDB table. Full contract: [[concepts/workflow-deployment-pipeline]].
- It requires the archive to contain exactly two flat files (a `.py` DAG + `meta.json`); this is the same contract this repo's release zip satisfies.

## Relationship to this repo

- This is the mechanism that actually deploys our released artifact. A release here is inert until an operator runs cape-cod-env pointing at (or bundling) our archive.
- Known quirk (theirs, affects us): workflow deploy is **not idempotent** - every run redeploys all workflows.

## Other responsibilities (context, not our concern day-to-day)

- `cape_env_db` role: runs `cape-cod-db` Alembic migrations; version in its `requirements.txt` must match target schema.
- `cape_env_resources` role: ingests tributaries/resources from a Pulumi deployment-state JSON into the DB.

See [[syntheses/cape-system-topology]].
