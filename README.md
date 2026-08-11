# CAPE Workflow: Bactopia + Kraken2 v3.2.0

[![CI/CD](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/actions/workflows/cape.yml/badge.svg)](https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/actions/workflows/cape.yml)

Bacterial genome taxonomic classification using Bactopia v3.2.0 and Kraken2.

## Overview

Airflow workflow that runs:

1. **Bactopia v3.2.0** - Genome assembly and analysis
2. **Kraken2 via Bactopia tools** - Taxonomic classification

Both steps run in AWS Batch as Nextflow pipelines.

## Pipeline Details

### Bactopia v3.2.0

Comprehensive bacterial genome analysis pipeline that performs:

- Quality control and trimming of raw sequencing reads
- De novo genome assembly
- Assembly quality assessment and annotation
- Taxonomic classification preparation

### Kraken2 Classification

Taxonomic classification tool that:

- Assigns taxonomic labels to DNA sequences using exact k-mer matches
- Compares assembled genomes against a reference database
- Generates classification reports with confidence scores
- Identifies contamination and validates sample identity

Both pipelines run as Nextflow workflows on AWS Batch, with Kraken2 using the shared database mounted at `/mnt/nextflow_shared_data/kraken2`.

## Requirements

- CAPE infrastructure with Airflow 3.0.6
- AWS Batch environment configured with:
  - Workflow queue for Nextflow head jobs
  - Analysis queue for pipeline tasks
  - Nextflow job definitions
- S3 buckets for pipeline input/output
- Kraken2 database mounted at `/mnt/nextflow_shared_data/kraken2`

## Usage

Triggered via the CAPE API (Airflow REST API) with a `dag_run.conf` object. The
supported pipeline IDs are declared in `meta.json`.

### Configuration

Pass configuration under `dag_run.conf` as a list of per-pipeline configs:

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
      "nextflowOptions": {
        "--wf": "kraken2",
        "--kraken2_db": "/mnt/nextflow_shared_data/kraken2"
      }
    }
  ]
}
```

Each entry has a `pipelineId` and a `nextflowOptions` dict (Nextflow CLI flag to
value). A validation task checks that the expected pipelines are present and that
required fields exist, then converts each `nextflowOptions` dict to a CLI string
for the AWS Batch jobs.

**Supported pipeline IDs and required options:**

- `bactopia-ont-v3.2.0` - requires `--sample` and `--outdir` (the output S3
  bucket is derived from `--outdir`)
- `bactopia-kraken2-v3.2.0` - no required options; Kraken2 flags such as `--wf`,
  `--bactopia`, and `--kraken2_db` are supplied here

## Development

See [AGENTS.md](AGENTS.md) for development setup, quality checks, and CI/CD information.

### Quick Start

```bash
# Setup
mise install
poetry install
pre-commit install

# Quality checks
black bactopia_kraken2_v3_2_0.py
isort bactopia_kraken2_v3_2_0.py
pyright bactopia_kraken2_v3_2_0.py
typos
```

## Documentation

- [AGENTS.md](AGENTS.md) - Development setup and CI/CD information
- [OPERATIONS.md](OPERATIONS.md) - Release management and troubleshooting

## Release Process

Releases are automated via semantic versioning:

- Use [conventional commits](https://www.conventionalcommits.org/) (feat:, fix:, etc.)
- Merge to main triggers version calculation and release
- GitHub releases include zip artifact with workflow files

## License

Apache-2.0

## Related Projects

- [cape-cod](https://github.com/cape-ph/cape-cod) - CAPE core infrastructure
- [capepy](https://github.com/cape-ph/capepy) - CAPE Python utilities
- [cape-frontend](https://github.com/cape-ph/cape-frontend) - CAPE web interface
