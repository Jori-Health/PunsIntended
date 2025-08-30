# Repo Map — Step 1 (Ingest & Normalize)

```
/.curso r/rules
/README.md
/repo_map.md
/docs/data_contracts.md
/schemas/note_raw.schema.json
/schemas/note_canonical.schema.json
/ingest/
  A/  B/  C/               # source-specific entrypoints
  common/provenance/       # helpers for lineage/provenance
/tests/fixtures/           # sample JSONL
```
Run order:
1) Ingest raw: Source A/B/C → `/data_lake/raw`
2) Normalize to canonical schema → `/data_lake/standardized`
3) Send bad rows to `/data_lake/rejected`
