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
1. **Create and push tag** (version is automatically derived from tag):
   ```bash
   git add -A
   git commit -m "Release v0.2.0"
   git tag v0.2.0
   git push origin main --tags
   ```

2. **Automatic PyPI Publishing**:
   - GitHub Actions detects the tag
   - Builds and publishes to PyPI automatically
   - Requires manual approval in the `pypi` environment

## Version Strategy

### Automatic Versioning
- **Version is automatically derived from Git tags** - no manual version updates needed!
- **Tagged commits**: Use the exact tag (e.g., `v0.2.0` → version `0.2.0`)
- **Development builds**: Auto-generate dev versions (e.g., `0.1.0b3.dev0+gf2b7d84`)

### Semantic Versioning
- **Patch** (0.1.1): Bug fixes, small improvements
- **Minor** (0.2.0): New features, backwards compatible  
- **Major** (1.0.0): Breaking changes

## Checklist Before Release

- [ ] All tests passing in CI
- [ ] Documentation updated  
- [ ] No breaking changes (or properly documented)
- [ ] Choose appropriate tag name (version is automatically derived from tag)
