# Operations Guide

Operational procedures and troubleshooting for workflow maintainers.

## Release Management

### Normal Release Process (Primary Method)

Releases are fully automated via conventional commits:

1. Create a branch from main
2. Make changes and commit with conventional commit message:
   - `feat:` - new feature (minor version bump)
   - `fix:` - bug fix (patch version bump)
   - `feat!:` or `BREAKING CHANGE:` - breaking change (major version bump)
3. Open PR to main
4. Merge PR after CI passes
5. Automated workflow creates release with artifacts

**Example:**
```bash
git checkout -b add-feature
# Make changes
git commit -m "feat: add support for custom database paths"
git push origin add-feature
# Open PR, get approval, merge to main
# Release v0.2.0 created automatically with artifacts
```

### Manual Artifact Upload (Emergency Recovery)

**When to use:** If a release exists but the artifact zip file failed to attach.

**Procedure:**

1. Navigate to: https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/actions/workflows/release.yml
2. Click "Run workflow" button (top right)
3. Select branch: `main`
4. Enter the release tag (e.g., `v1.2.3`)
5. Click "Run workflow"
6. Wait for workflow completion (~1-2 minutes)
7. Verify artifact appears in the release assets

**Expected artifact naming:** `cape-wf-bactopia_kraken2_v3_2_0-{tag}.zip`

**Example:** For release `v1.2.3`, artifact should be named `cape-wf-bactopia_kraken2_v3_2_0-v1.2.3.zip`

**Note:** This should rarely be needed. The automated flow handles artifact attachment. Use only if automated attachment fails or if manually recreating a release.

## Troubleshooting

### Release created but no custom artifact

**Symptoms:**
- GitHub release exists with tag (e.g., v1.2.3)
- Only default source archives present (v1.2.3.zip, v1.2.3.tar.gz)
- Missing custom workflow artifact

**Diagnosis:**
1. Check workflow run: Actions → Semantic Versioning Release
2. Look for `attach-artifacts` job
3. Check job logs for errors

**Resolution:**
- Use manual artifact upload procedure (see above)
- If repeated failures, check workflow permissions and file paths

### Release not created after merge to main

**Symptoms:**
- PR merged to main
- No new release created
- No new git tag

**Diagnosis:**
1. Check commit message follows conventional commit format
2. Verify PR was merged (not closed without merge)
3. Check workflow run logs for release-please output

**Common causes:**
- Non-conventional commit message (e.g., "Updated feature" instead of "feat: updated feature")
- No changes in scope that warrant release (chore commits don't trigger releases by default)

**Resolution:**
- If commit message was wrong, make a new commit with proper format
- Check CHANGELOG.md to see what release-please detected

### CI checks failing on PR

**Symptoms:**
- PR checks show failures
- Cannot merge to main

**Diagnosis:**
Check which check failed:
- **Pyright:** Type errors in code
- **Black:** Code formatting issues
- **isort:** Import sorting issues  
- **typos:** Spelling errors in comments/docs
- **Conventional Commit:** PR title doesn't follow format

**Resolution:**
```bash
# Run locally to identify and fix
black bactopia_kraken2_v3_2_0.py
isort bactopia_kraken2_v3_2_0.py
pyright bactopia_kraken2_v3_2_0.py
typos

# Fix issues and commit
git add .
git commit -m "fix: correct type errors and formatting"
git push
```

## GitHub Actions Permissions

**Required permissions for release workflow:**
- `contents: write` - Create releases and tags
- `pull-requests: write` - Comment on PRs and manage release PR

These are configured in `.github/workflows/release.yml`.

**Required secrets:**
- `GITHUB_TOKEN` - Automatically provided by GitHub Actions (no setup needed)

## Emergency Procedures

### Deleting a Bad Release

If a release was created incorrectly:

```bash
# Delete release (keeps tag)
gh release delete v1.2.3

# Delete tag locally and remotely
git tag -d v1.2.3
git push origin :refs/tags/v1.2.3

# Fix issues, then create proper release via normal flow
```

### Rolling Back a Release

Workflow releases are immutable artifacts - rolling back means:

1. Identify the last good release (e.g., v1.2.0)
2. Create a new release that reverts changes:
   ```bash
   git revert <bad-commit-sha>
   git commit -m "fix: revert problematic changes from v1.2.3"
   # This creates v1.2.4 with the revert
   ```

**Do not delete releases** that have been deployed/used. Create forward-fixing releases instead.

## Monitoring

**Key metrics to watch:**
- Release workflow success rate
- Time from merge to release creation
- Artifact upload success rate

**Where to check:**
- GitHub Actions: https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/actions
- Releases page: https://github.com/cape-ph/cape-wf-bactopia_kraken2_v3_2_0/releases

## Related Documentation

- [AGENTS.md](AGENTS.md) - Development setup and conventions
- [README.md](README.md) - User-facing workflow documentation
- [Conventional Commits](https://www.conventionalcommits.org/) - Commit message format
- [Release Please](https://github.com/googleapis/release-please) - Release automation tool
