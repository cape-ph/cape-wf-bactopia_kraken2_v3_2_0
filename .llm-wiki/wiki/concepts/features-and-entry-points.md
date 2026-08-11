---
type: concept
title: Features and Entry Points
created: 2026-08-11
updated: 2026-08-11
---

# Features and Entry Points

## Primary entry point

`bactopia_kraken2_v3_2_0.py` -> `bactopia_and_kraken2_v3_2_0()` DAG. Everything the repo does happens through this one DAG. See [[entities/bactopia-kraken2-dag]].

## Feature: config validation and extraction

- Implemented by the `validate_and_extract_nextflow_configs` TaskFlow task (first in the chain).
- Validates `dag_run.conf.pipelineConfigs` against `EXPECTED_PIPELINES`, flattens each stage's `nextflowOptions` to a CLI string, and publishes the result via XCom for the downstream operators. See [[concepts/metajson-and-parameters]].

## Feature: Bactopia genome analysis stage

- Implemented by the `submit_bactopia_batch_job` task.
- Runs Bactopia v3.2.0 (QC/trimming, de novo assembly, assembly QA/annotation) as a Nextflow pipeline on AWS Batch.
- Output goes to the S3 location from the bactopia `--outdir`; the bucket is parsed out with the `extract_s3_bucket` Jinja filter.

## Feature: Kraken2 taxonomic classification stage

- Implemented by `submit_kraken2_batch_job`.
- Runs Kraken2 as a Bactopia tool against the Bactopia output; the `--wf kraken2`, `--bactopia`, and `--kraken2_db` flags come from the user's kraken2 `nextflowOptions`.
- Typically uses the shared Kraken2 DB mounted at `/mnt/nextflow_shared_data/kraken2`.
- Assigns taxonomy by exact k-mer matches; used for contamination detection and sample-identity validation.

## Feature: stage coordination via S3 sentinel

- `create_k2_include` (writes the include file) + `wait_for_kraken_2_include_file` (`S3KeySensor`) gate the Kraken2 stage on an S3 marker. Detailed in [[concepts/workflow-task-flow]].

## Not yet implemented

- Report generation (explicit `TODO` after the Kraken2 job, with storage/authz concerns noted).
- Parameterized (non-hard-coded) queue and job-definition names.
- Sourcing pipeline schema/project/version from the CAPE `/workflows/pipelineprofiles` endpoint to drop the hard-coded project/version constants (issue #10).

## Interface to CAPE

- `meta.json` declares `dag_id` and `pipeline_ids` used by the CAPE frontend/backend. See [[concepts/cape-platform-integration]].
