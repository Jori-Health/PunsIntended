# Step 1: Ingest & Normalize System

A modular ingest and normalize system for processing JSONL files from multiple sources into a standardized data lake format.

## Overview

This system implements:
- **Ingest**: Copy and validate raw JSONL files → `/data_lake/raw`
- **Normalize**: Transform into canonical schema → `/data_lake/standardized`
- **Rejected records**: Invalid data → `/data_lake/rejected`
- **Provenance tracking**: Lineage information for all processed data

## Architecture

```
ingest/
├── common/                 # Shared utilities
│   ├── ingest_engine.py   # Core ingest functionality
│   ├── normalize_engine.py # Core normalize functionality
│   ├── schema_validator.py # JSON schema validation
│   ├── provenance.py      # Provenance tracking
│   └── io_utils.py        # File I/O utilities
├── A/                     # Source A connector
│   └── cli.py            # CLI for Source A
├── B/                     # Source B connector
│   └── cli.py            # CLI for Source B
├── C/                     # Source C connector
│   └── cli.py            # CLI for Source C
└── main.py               # Main orchestrator

data_lake/
├── raw/                  # Raw ingested data
├── standardized/         # Canonical format data
├── staging/             # Intermediate data (future use)
└── rejected/            # Invalid records with error reasons
```

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure the data lake directory structure exists:
```bash
mkdir -p data_lake/{raw,staging,standardized,rejected}
```

## Usage

### Individual Source Connectors

Each source (A, B, C) has its own CLI for processing:

```bash
# Process Source A
python -m ingest.A.cli ingest sources/A --normalize

# Process Source B
python -m ingest.B.cli ingest sources/B --normalize

# Process Source C
python -m ingest.C.cli ingest sources/C --normalize
```

### Main Orchestrator

Process all sources at once:

```bash
# Process all sources
python -m ingest.main --normalize

# Process specific sources
python -m ingest.main --sources A C --normalize

# Verbose logging
python -m ingest.main --verbose --normalize
```

### CLI Options

All CLIs support the following options:

- `--verbose, -v`: Enable verbose logging
- `--data-lake-root`: Data lake root directory (default: `data_lake`)
- `--schema-path`: Raw schema file path (default: `schemas/note_raw.schema.json`)
- `--canonical-schema-path`: Canonical schema file path (default: `schemas/note_canonical.schema.json`)
- `--normalize`: Also run normalization after ingest

## Data Flow

1. **Ingest Phase**:
   - Read JSONL files from `/sources/{A|B|C}/*.jsonl`
   - Validate against `schemas/note_raw.schema.json`
   - Write valid records to `/data_lake/raw/`
   - Write rejected records to `/data_lake/rejected/`
   - Generate lineage files with provenance information

2. **Normalize Phase**:
   - Read raw files from `/data_lake/raw/`
   - Transform to canonical format
   - Validate against `schemas/note_canonical.schema.json`
   - Write standardized records to `/data_lake/standardized/`
   - Write rejected records to `/data_lake/rejected/`

## Schema Validation

The system validates data against two schemas:

- **Raw Schema** (`schemas/note_raw.schema.json`): Validates incoming JSONL records
- **Canonical Schema** (`schemas/note_canonical.schema.json`): Validates transformed records

## Provenance Tracking

Each ingested file generates a lineage file containing:
- Source file path
- Checksum
- Ingest timestamp
- Processing statistics
- Output file locations

## Testing

Run the test suite:

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_ingest.py

# Run with verbose output
pytest -v tests/
```

The tests use fixtures from `tests/fixtures/` to validate the system against sample data.

## Output Structure

After processing, the data lake contains:

```
data_lake/
├── raw/
│   ├── A.sample_raw_20241201_120000.jsonl
│   ├── A.sample_lineage.json
│   └── ...
├── standardized/
│   ├── A.sample_raw_20241201_120000_canonical_20241201_120100.jsonl
│   └── ...
└── rejected/
    ├── A.sample_rejected_20241201_120000.jsonl
    └── ...
```

## Error Handling

- Invalid JSON records are logged and skipped
- Schema validation failures are written to `/data_lake/rejected/`
- Transformation errors are captured with detailed error messages
- All errors are logged with structured logging

## Constraints

- Python 3.11+
- No external services (local filesystem only)
- Minimal dependencies (stdlib + jsonschema + pytest)
- Small, pure functions with clear I/O signatures
- Structured logging to stdout
