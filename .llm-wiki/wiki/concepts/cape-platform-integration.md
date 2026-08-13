---
type: concept
title: CAPE Platform Integration
created: 2026-08-11
updated: 2026-08-11
---

# CAPE Platform Integration

How this workflow fits the wider CAPE (Center for Applied Pathogen Genomics, "and Outbreak Control") ecosystem. For the concrete build/deploy relationship between this repo and its infra/deploy siblings, see [[syntheses/cape-system-topology]].

## Role

- One member of the `cape-wf-*` workflow family. Each such repo is a self-contained Airflow DAG plus a `meta.json` interface descriptor.
- CAPE deploys the DAG into a pre-built Airflow 3.1+ environment; this repo does not own that environment or its dependencies.

## Interface

- `meta.json` exposes `dag_id` and `pipeline_ids` so the CAPE frontend/backend can associate user-facing pipelines with this DAG. See [[concepts/metajson-and-parameters]].
- The CAPE frontend collects user inputs (driven by JSON schema from the backend) and triggers the DAG through the CAPE API, passing the params consumed in [[concepts/workflow-task-flow]].

## Related CAPE projects

- **cape-cod** - Pulumi IaC that defines the AWS environment (Airflow, Batch, S3, DynamoDB registries) this DAG runs in. See [[entities/cape-cod]].
- **cape-cod-env** - Ansible repo that deploys the released workflow archive onto that infrastructure. See [[entities/cape-cod-env]] and [[concepts/workflow-deployment-pipeline]].
- **capepy** - CAPE Python utilities (planned future import here).
- **cape-frontend** - CAPE web interface; consumes `parametersSchema` from cape-cod's analysis-pipeline registry to collect params.
- Shared reusable CI lives in `cape-ph/.github` (referenced by this repo's workflows).

## Open integration questions (from code comments)

- Final shape of the per-DAP config dict sent by the API.
- How DAP/pipeline versioning maps onto workflows without exploding into one workflow per version combination; a `pipeline-id` key is floated as a possibility.

See [[syntheses/architecture-overview]] and [[concepts/external-dependencies-and-boundaries]].
