---
type: concept
title: Workflow Task Flow
created: 2026-08-11
updated: 2026-08-11
---

# Workflow Task Flow

The DAG in [[entities/bactopia-kraken2-dag]] wires a validation task plus four operator tasks in a linear `chain(...)`:

```
validate_and_extract_nextflow_configs -> submit_bactopia_job -> create_k2_include -> wait_for_k2_include -> submit_kraken2_job
```

## Tasks

0. **validate_and_extract_nextflow_configs** (TaskFlow `@task`)
   - Reads `dag_run.conf` (structure in [[concepts/metajson-and-parameters]]).
   - Validates that each `EXPECTED_PIPELINES` stage is present by `pipelineId` and that its `required_fields` exist in `nextflowOptions`.
   - Flattens each stage's `nextflowOptions` dict to a CLI string (`nextflowOptions_to_cli_string`) and returns a `{bactopia, kraken2}` dict (with `pipelineId`, `nextflowOptions`, `nextflowOptionsCli`) to downstream tasks via **XCom**.
   - `fail_on_any_error` (default `True`) added for future library reuse.

1. **submit_bactopia_batch_job** (`BatchOperator`)
   - Submits the Bactopia Nextflow pipeline to `WORKFLOW_QUEUE_NAME` using `NEXTFLOW_JOB_DEFINITION`.
   - Passes `PIPELINE`, `PIPELINE_VERSION`, `PIPELINE_QUEUE`, and `NF_OPTS` as container env overrides.
   - `NF_OPTS` is now an XCom pull of the bactopia `nextflowOptionsCli` from the validation task.

2. **create_k2_include** (`S3CreateObjectOperator`)
   - Writes the sample id (XCom pull of bactopia `--sample`) to `batch_job_scratch/kraken2/{dag_run.dag_id}-k2-include.txt`.
   - Target bucket is derived from the bactopia `--outdir` via the `extract_s3_bucket` Jinja filter (registered on the DAG as `user_defined_filters`).
   - `replace=True`.

3. **wait_for_kraken_2_include_file** (`S3KeySensor`)
   - Pokes the same bucket/key for the include file; `poke_interval = 10` seconds.

4. **submit_kraken2_batch_job** (`BatchOperator`)
   - `NF_OPTS` composes `--aws_queue <JOB_QUEUE_NAME>`, the kraken2 `nextflowOptionsCli` (XCom), a computed `--include s3://<bucket>/<include-key>`, and `--aws_volumes` bind mounts.
   - Kraken2-specific flags (`--wf kraken2`, `--bactopia`, `--kraken2_db`) now come from the user's kraken2 `nextflowOptions`, not hard-coded here.

## Notes

- Constants `K2_INCLUDE_PREFIX` / `K2_INCLUDE_SUFFIX` build the include-file key.
- Report generation is still a placeholder `TODO` after the Kraken2 job (now with notes on storage/authz concerns).
- S3 sentinel-file coordination is the mechanism that gates Kraken2 on Bactopia readiness.
- Config values come from [[concepts/metajson-and-parameters]]; external systems from [[concepts/external-dependencies-and-boundaries]].
