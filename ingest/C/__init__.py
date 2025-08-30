# Placeholder module stub for Cursor to implement.
# TODO:
# - Implement CLI: python -m this_package ingest <input_path> <output_root>
# - Validate against schemas/note_raw.schema.json
# - Write raw copy to /data_lake/raw with lineage.json
# - Transform to canonical (schemas/note_canonical.schema.json) → /data_lake/standardized
# - On validation error → write to /data_lake/rejected with error reason
