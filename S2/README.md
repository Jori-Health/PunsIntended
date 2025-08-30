# Step 2: Identity Resolution & Unit-of-Record

This module implements deterministic patient identity resolution across multiple data sources to create a unified patient unit-of-record (UoR).

## Overview

The identity resolution system takes standardized notes from Step 1 and reconciles them into patient records with stable `patient_uid` values that are resolvable across sources.

## Architecture

```
resolve/
├── matchers/          # Identity matching logic
│   ├── __init__.py
│   └── identity_matcher.py
├── writers/           # Output writers
│   ├── __init__.py
│   └── data_writer.py
└── rules/            # Main orchestration
    ├── __init__.py
    └── (CLI entry points)
```

## Matching Strategy

1. **Primary Match**: Normalized MRN match
   - MRNs are normalized per source (removes prefixes like "MDA-", "BMC-", "CANCER-")
   - Highest confidence (0.9)

2. **Secondary Match**: Triplet match (DOB + sex + diagnosis substring)
   - Used when MRN is missing or inconsistent
   - Medium confidence (0.7)

3. **New Patient**: Create new patient record
   - When no matches found
   - Deterministic UID generation from available identifiers

## Usage

### Build Patient Unit-of-Record

```bash
python -m resolve.rules build <standardized_dir> <warehouse_root> <artifacts_root>
```

**Arguments:**
- `standardized_dir`: Path to standardized notes from Step 1
- `warehouse_root`: Root directory for data warehouse outputs
- `artifacts_root`: Root directory for artifacts and reports

**Outputs:**
- `/data_warehouse/patients/{date}/patients.jsonl` - Patient unit-of-record
- `/data_warehouse/links/{date}/note_links.jsonl` - Note-to-patient links
- `/data_warehouse/conflicts/{date}/conflicts.jsonl` - Resolution conflicts
- `/artifacts/identity/{date}/report.json` - Processing report

### View Report

```bash
python -m resolve.rules report <report.json>
```

## Data Schemas

### Patient Record Schema
```json
{
  "patient_uid": "string",
  "mrn_set": ["string"],
  "demographics": {
    "sex": "string",
    "dob": "string"
  },
  "diagnoses": ["string"],
  "provenance": {
    "notes_linked": "integer",
    "sources": ["string"]
  }
}
```

### Note Link Schema
```json
{
  "note_uid": "string",
  "patient_uid": "string",
  "rule": "string",
  "mrn": "string",
  "source_id": "string"
}
```

## Conflict Handling

The system detects and reports conflicts when:
- Same MRN maps to different patients
- Same demographic triplet maps to different patients

Conflicts are written to `/data_warehouse/conflicts/{date}/conflicts.jsonl` for manual review.

## Example

```bash
# Build patient records from Step 1 output
python -m resolve.rules build \
  ../S1/step1/data_lake/standardized \
  ./data_warehouse \
  ./artifacts

# View the processing report
python -m resolve.rules report ./artifacts/identity/20241201/report.json
```

## Testing

Run the test suite:

```bash
pytest tests/
```

The tests verify:
- Build command creates all expected outputs
- Report command displays results correctly
- Identity resolution logic works as expected


---

### 6. Tests  
`$LOCAL/S2/tests/conftest.py`
```