---
type: concept
title: meta.json and Parameters
created: 2026-08-11
updated: 2026-08-11
---

# meta.json and Parameters

`meta.json` is the workflow's interface contract with the CAPE frontend/backend.

## Current contents

```json
{
    "dag_id": "bactopia_kraken2_v3_2_0",
    "pipeline_ids": ["bactopia-ont-v3.2.0", "bactopia-kraken2-v3.2.0"]
}
```

- `dag_id` must match `DAG_ID` in the DAG file. It is the join key used across the system: the deployed S3 object name (`<dag_id>.py`), the Airflow DAG id, and the workflow-meta-registry DynamoDB hash key. A mismatch caused downstream system issues, fixed in v0.1.3. See [[syntheses/cape-system-topology]].
- `pipeline_ids` associates this DAG with CAPE analysis-pipeline identities. Each must match a `pipeline_id` registered in cape-cod's analysis-pipeline registry; the frontend uses that registry entry's `parametersSchema` to collect user parameters. cape-cod-env writes `{dag_id, pipeline_ids}` into the workflow-meta registry at deploy time (see [[concepts/workflow-deployment-pipeline]]).

## Runtime configuration (`dag_run.conf`, as of the #9 config-format rework)

The DAG is triggered via the Airflow API with a `conf` object (not templated `params`). Structure:

```json
{
    "pipelineConfigs": [
        {
            "pipelineId": "bactopia-ont-v3.2.0",
            "nextflowOptions": {
                "--sample": "sample-001",
                "--outdir": "s3://my-bucket/bactopia-output"
            }
        },
        {
            "pipelineId": "bactopia-kraken2-v3.2.0",
            "nextflowOptions": { "--wf": "kraken2", "--kraken2_db": "/mnt/nextflow_shared_data/kraken2" }
        }
    ]
}
```

- `pipelineConfigs` is a list; each entry has a `pipelineId` and a free-form `nextflowOptions` dict of Nextflow CLI flag -> value.
- The `validate_and_extract_nextflow_configs` task validates and reshapes this into per-stage entries keyed `bactopia` / `kraken2`, each with `pipelineId`, `nextflowOptions`, and a flattened `nextflowOptionsCli` string, published via XCom. See [[concepts/workflow-task-flow]].
- The `pipelineId` values here are the same identities as `meta.json` `pipeline_ids` and cape-cod's analysis-pipeline registry `pipeline_id`s.

### Expected pipelines and required fields (`EXPECTED_PIPELINES` constant)

| stage key | accepted `pipelineId`s | required `nextflowOptions` |
|-----------|------------------------|----------------------------|
| `bactopia` | `bactopia-ont-v3.2.0` | `--sample`, `--outdir` |
| `kraken2` | `bactopia-kraken2-v3.2.0` | (none) |

- The output S3 bucket is now **derived from the bactopia `--outdir`** (an `s3://` URL) via the `extract_s3_bucket` Jinja filter, not supplied as a separate bucket parameter.
- Kraken2-specific flags (`--wf kraken2`, `--bactopia`, `--kraken2_db`) now come from the user's kraken2 `nextflowOptions` rather than being hard-coded in the DAG.

## Open design questions (documented in code comments)

- Required fields currently live in the `EXPECTED_PIPELINES` constant. The TODO plan (issue #10) is to source the schema, `pipelineId`, `project`, and `version` from the CAPE `/workflows/pipelineprofiles` endpoint instead, which would let the `BACTOPIA_PROJ` / `BACTOPIA_VERSION` / `KRAKEN2_PROJ` / `KRAKEN2_VERSION` constants be removed.
- XCom size limits for large `nextflowOptions` configs are noted as needing investigation.

> Note: `README.md` still documents the older `params.bactopia.*` / `params.kraken2.*` format and is stale relative to the code; the `dag_run.conf` / `pipelineConfigs` structure above is authoritative.

See [[concepts/workflow-task-flow]] for where each config value is consumed and [[concepts/cape-platform-integration]] for the platform side.
