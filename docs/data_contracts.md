# Data Contracts (Step 1)

## Raw Note (per-line JSON)
- `patient_id`: string (e.g., "P001")
- `mrn`: string
- `source`: enum {"Source-A","Source-B","Source-C"}
- `oncologist_id`: string
- `oncologist_name`: string
- `note_datetime`: ISO 8601 datetime
- `section`: string
- `text`: string
- `provenance.ingested_from`: string path
- `provenance.checksum`: string

## Canonical Note
- `uid`: string (deterministic hash of source+mrn+timestamp+section)
- `patient_id`: string
- `mrn`: string
- `source_id`: string
- `encounter.ts`: ISO 8601 datetime
- `encounter.section`: string
- `content.raw_text`: string
- `provenance`: { source, oncologist_id, oncologist_name, ingested_from, checksum }
- `validation`: { status: "ok"|"rejected", errors?: [string] }
