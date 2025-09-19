# Release Process

## Setup (One-time)

### Configure Trusted Publishing (Recommended - No API tokens needed!)

1. **For PyPI**: Go to https://pypi.org/manage/account/publishing/
   - Project name: `aio-sf`
   - Owner: `your-github-username`
   - Repository name: `salesforce-to-s3` (or your repo name)
   - Workflow filename: `publish.yml`
   - Environment name: `pypi`

2. **For TestPyPI**: Go to https://test.pypi.org/manage/account/publishing/
   - Same details but Environment name: `testpypi`

3. **Create GitHub Environments**:
   - Go to your repo → Settings → Environments
   - Create `pypi` environment (require manual approval for security)
   - Create `testpypi` environment (no approval needed)

## Release Process

### Every Push → TestPyPI
- **Automatic**: Every push to any branch publishes to TestPyPI
- **Purpose**: Test your packaging pipeline

### Tagged Push → PyPI  
1. **Update version** in `src/aio_sf/__init__.py`:
   ```python
   __version__ = "0.2.0"  # Update this
   ```

2. **Create and push tag**:
   ```bash
   git add -A
   git commit -m "Release v0.2.0"
   git tag v0.2.0
   git push origin main --tags
   ```

3. **Automatic PyPI Publishing**:
   - GitHub Actions detects the tag
   - Builds and publishes to PyPI automatically
   - Requires manual approval in the `pypi` environment

## Manual Release (Backup)

If you need to publish manually:

```bash
# Build the package
uv build

# Publish to PyPI (requires PYPI_API_TOKEN env var)
export PYPI_API_TOKEN=your_token_here
uv publish --token $PYPI_API_TOKEN
```

## Test Release

To test on TestPyPI first:

```bash
# Get TestPyPI token from test.pypi.org
uv publish --repository testpypi --token $TEST_PYPI_TOKEN

# Test install from TestPyPI
pip install --index-url https://test.pypi.org/simple/ aio-salesforce
```

## Version Strategy

- **Patch** (0.1.1): Bug fixes, small improvements
- **Minor** (0.2.0): New features, backwards compatible
- **Major** (1.0.0): Breaking changes

## Checklist Before Release

- [ ] Update version in `__init__.py`
- [ ] Update CHANGELOG (if you have one)
- [ ] All tests passing in CI
- [ ] Documentation updated
- [ ] No breaking changes (or properly documented)
