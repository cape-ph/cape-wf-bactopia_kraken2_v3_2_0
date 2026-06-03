# AGENTS.md

## Repository structure

CAPE workflow for Bactopia v3.2.0 and Kraken2 taxonomic classification.

- `bactopia_kraken2_v3_2_0.py` - Airflow DAG workflow definition
- `meta.json` - workflow metadata (inputs, outputs, parameters)
- `README.md` - user-facing documentation
- `pyproject.toml` - tool configuration (Black, isort, Ruff) and CI dependencies
- `.github/workflows/` - CI/CD automation

## Development environment

### Setup

Install tools and dependencies:
```bash
mise install          # Install Python, Poetry, pre-commit, opencode
poetry install        # Install dependencies for type checking
pre-commit install    # Enable git hooks
```

## Quality checks

Run locally before committing:
```bash
# Format code
black bactopia_kraken2_v3_2_0.py
isort bactopia_kraken2_v3_2_0.py

# Type check
pyright bactopia_kraken2_v3_2_0.py

# Spell check
typos
```

Pre-commit hooks run these automatically on git commit.

## CI/CD automation

### Pull request checks

PRs trigger:
- Pyright type checking (basic mode, ignores missing stubs)
- Black formatting validation (80 char lines)
- isort import sorting validation (Black-compatible profile)
- typos spell checking
- Conventional commit PR title validation

All checks must pass before merge.

### Release process

Automated via release-please:

1. Use conventional commits:
   - `feat:` - new feature (minor version bump)
   - `fix:` - bug fix (patch version bump)
   - `feat!:` or `BREAKING CHANGE:` - breaking change (major version bump)

2. Merge to main triggers:
   - Semantic version calculation
   - CHANGELOG.md update
   - Git tag creation (e.g., v1.2.3)
   - GitHub release with zip artifact: `cape-wf-bactopia_kraken2_v3_2_0-v1.2.3.zip`

3. Release artifact contains:
   - `bactopia_kraken2_v3_2_0.py`
   - `meta.json`

**See [OPERATIONS.md](OPERATIONS.md) for detailed release procedures and troubleshooting.**

## Workflow conventions

- Workflow is an Airflow DAG using taskflow API
- Requires Airflow 3.1+ with Amazon provider
- Runs in pre-built CAPE environment (dependencies not managed here)
- `meta.json` defines workflow interface for CAPE frontend
- No unit tests (workflow validated via integration testing in CAPE environment)

## Tool configurations

- Black: 80 character line length
- isort: Black-compatible profile, 80 char lines
- Pyright: Basic type checking, ignores missing stubs
- Python: 3.10+
- Opencode: Pinned to v1.x (auto-updates within major version)

## Dependencies

Listed in `pyproject.toml` only for CI type checking:
- boto3 - AWS SDK
- apache-airflow[amazon] - Airflow with AWS providers
- Eventually: capepy - CAPE utilities

These are NOT bundled or distributed - workflow runs in pre-built CAPE environments.

## Notes

- This workflow is not published to PyPI (use GitHub releases)
- No Sphinx documentation (README is sufficient)
- No pytest tests (integration tested in CAPE environment)
- Related projects: cape-cod, capepy, cape-frontend
- Part of cape-ph workflow collection (cape-wf-* pattern)
