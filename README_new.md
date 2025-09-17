# salesforce-to-s3

[![CI](https://github.com/callawaycloud/salesforce-to-s3/actions/workflows/ci.yml/badge.svg)](https://github.com/callawaycloud/salesforce-to-s3/actions/workflows/ci.yml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A modern Python library for exporting Salesforce SObjects to S3 as Parquet files.

## Features

- 🚀 **Modern Python**: Built with Python 3.9+ using type hints and async support
- 📦 **Parquet Format**: Efficient columnar storage with compression
- ☁️ **S3 Integration**: Direct upload to Amazon S3 with configurable paths
- 🔧 **Flexible Configuration**: CLI arguments, environment variables, or JSON config files
- 🛡️ **Type Safety**: Full type annotations with mypy validation
- 🧪 **Well Tested**: Comprehensive test suite with pytest
- 📝 **Modern Tooling**: Uses ruff for linting/formatting, uv for dependency management

## Installation

Using uv (recommended):
```bash
uv add salesforce-to-s3
```

Using pip:
```bash
pip install salesforce-to-s3
```

## Quick Start

### Command Line Usage

```bash
# Export using command line arguments
salesforce-to-s3 \
    --username "your-sf-username" \
    --password "your-sf-password" \
    --security-token "your-sf-token" \
    --s3-bucket "your-s3-bucket" \
    --sobjects Account \
    --sobjects Contact \
    --verbose

# Export using environment variables
export SF_USERNAME="your-sf-username"
export SF_PASSWORD="your-sf-password"
export SF_SECURITY_TOKEN="your-sf-token"
export S3_BUCKET="your-s3-bucket"

salesforce-to-s3 --sobjects Account --sobjects Contact
```

### Configuration File

Create a `config.json` file:
```json
{
    "username": "your-sf-username",
    "password": "your-sf-password", 
    "security_token": "your-sf-token",
    "s3_bucket": "your-s3-bucket",
    "s3_prefix": "salesforce-exports",
    "sobjects": ["Account", "Contact", "Opportunity"],
    "batch_size": 10000,
    "compression": "snappy"
}
```

Then run:
```bash
salesforce-to-s3 --config config.json
```

### Python API

```python
from salesforce_to_s3 import SalesforceToS3Exporter
from salesforce_to_s3.core import ExportConfig

# Create configuration
config = ExportConfig(
    username="your-sf-username",
    password="your-sf-password",
    security_token="your-sf-token",
    s3_bucket="your-s3-bucket",
    sobjects=["Account", "Contact"],
)

# Create exporter and run
exporter = SalesforceToS3Exporter(config)
results = exporter.export_all()

for result in results:
    print(f"Exported {result['records_exported']} {result['sobject']} records")
```

## Configuration Options

| Option | CLI Flag | Environment Variable | Description |
|--------|----------|---------------------|-------------|
| Username | `--username` | `SF_USERNAME` | Salesforce username |
| Password | `--password` | `SF_PASSWORD` | Salesforce password |
| Security Token | `--security-token` | `SF_SECURITY_TOKEN` | Salesforce security token |
| Domain | `--domain` | `SF_DOMAIN` | Salesforce domain (login/test) |
| S3 Bucket | `--s3-bucket` | `S3_BUCKET` | Target S3 bucket name |
| S3 Prefix | `--s3-prefix` | `S3_PREFIX` | S3 object prefix |
| AWS Region | `--aws-region` | `AWS_REGION` | AWS region |
| SObjects | `--sobjects` | - | SObject types to export |
| Batch Size | `--batch-size` | - | Records per batch |
| Compression | `--compression` | - | Parquet compression (snappy/gzip/brotli/lz4) |

## Development

This project uses modern Python tooling:

- **uv** for dependency management and virtual environments
- **ruff** for linting and code formatting
- **mypy** for type checking
- **pytest** for testing
- **pre-commit** for git hooks

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/callawaycloud/salesforce-to-s3.git
cd salesforce-to-s3

# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync --all-extras

# Install pre-commit hooks
uv run pre-commit install
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=salesforce_to_s3

# Run specific test file
uv run pytest tests/test_core.py
```

### Code Quality

```bash
# Lint and format
uv run ruff check .
uv run ruff format .

# Type checking
uv run mypy src/salesforce_to_s3

# Run all quality checks
uv run pre-commit run --all-files
```

### Building

```bash
# Build distribution packages
uv build
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.