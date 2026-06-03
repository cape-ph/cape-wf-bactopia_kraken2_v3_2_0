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

- CAPE infrastructure with Airflow 3.1+
- AWS Batch environment configured with:
  - Workflow queue for Nextflow head jobs
  - Analysis queue for pipeline tasks
  - Nextflow job definitions
- S3 buckets for pipeline input/output
- Kraken2 database mounted at `/mnt/nextflow_shared_data/kraken2`

## Usage

Triggered via CAPE API with workflow-specific parameters defined in `meta.json`.

### Parameters

The workflow expects configuration for two pipeline stages:

**Bactopia parameters:**
- `pipelineOutputBucket` - S3 bucket for outputs
- `pipelineOutputPrefix` - Prefix path in bucket
- `--sample` - Sample identifier
- `nextflowOptions` - Additional Nextflow CLI options

**Kraken2 parameters:**
- `pipelineOutputBucketName` - S3 bucket name for scratch files
- `nextflowOptions` - Additional Nextflow CLI options

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
