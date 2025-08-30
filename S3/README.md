# Stage 3 - Retrieval & Ranking (Semantic Search X)

This project implements a three-stage retrieval and ranking pipeline for semantic search.

## Overview

The pipeline consists of three stages:

### Stage A: Scouts (BM25 + Dense)
- Performs BM25 and dense search on the query
- Merges and fuses results to get top K_A≈200 candidates
- Normalizes scores to [0,1] range
- Output: `candidates.jsonl` with chunk_id, s_bm25, s_dense, fusion_score

### Stage B: Inspectors (Late-interaction/Neural-sparse)
- Re-scores the top 200 candidates using late interaction scoring
- Output: `rescored.jsonl` with top 50 candidates including s_li and optional evidence

### Stage C: Judges (Cross-encoder)
- Cross-encodes (query, chunk_text) pairs
- Calibrates scores using isotonic/Platt scaling
- Attaches patient_uid if note_links provided
- Output: `final.jsonl` with top 10 results and calibrated scores

## Usage

### Full Pipeline
```bash
make stage3
```

### Custom Query
```bash
make query Q="your search query"
```

### Individual Stages
```bash
# Stage A
python3 -m stageA run <bm25_dir> <dense_dir> <chunks_jsonl> "<query>" <out_dir>

# Stage B  
python3 -m stageB run <candidates.jsonl> <chunks_jsonl> <out_dir>

# Stage C
python3 -m stageC run <rescored.jsonl> <chunks_jsonl> <out_dir> [--links <note_links.jsonl>]
```

## Configuration

Configuration is stored in `configs/retrieval.yaml`:

- K_A: 200 (Stage A candidates)
- K_B: 50 (Stage B candidates)  
- K_C: 10 (Stage C final results)
- BM25 parameters: k1=0.9, b=0.4
- Fusion method: weighted_sum with w_bm25=0.5, w_dense=0.5

## File Structure

```
├── stageA/__init__.py          # Scouts implementation
├── stageB/__init__.py          # Inspectors implementation  
├── stageC/__init__.py          # Judges implementation
├── retrieve/
│   ├── fusion/combiner.py      # Score fusion and normalization
│   └── utils/io.py            # I/O utilities
├── configs/retrieval.yaml      # Configuration
├── tests/                      # Test fixtures and tests
└── Makefile                   # Pipeline orchestration
```

## Testing

Run the test suite:
```bash
python3 tests/test_stage3.py
```

## Output Format

### Stage A Output (candidates.jsonl)
```json
{
  "chunk_id": "chunk_001",
  "s_bm25": 0.8,
  "s_dense": 0.6,
  "fusion_score": 0.7,
  "source_id": "note_001",
  "note_uid": "note_001"
}
```

### Stage B Output (rescored.jsonl)
```json
{
  "chunk_id": "chunk_001", 
  "s_li": 0.75,
  "fusion_score": 0.7,
  "evidence": [
    {"token": "progression", "weight": 0.4, "pos": 5}
  ]
}
```

### Stage C Output (final.jsonl)
```json
{
  "chunk_id": "chunk_001",
  "calibrated_score": 0.85,
  "patient_uid": "patient_123",
  "pointers": {
    "chunk_offset": 0,
    "file": "patient_notes.txt"
  }
}
```

## Implementation Notes

- All scoring functions are currently stubs that simulate real ML models
- Score normalization uses min-max scaling
- Fusion uses weighted sum of normalized scores
- Patient UID attachment is optional and requires note_links.jsonl
- All stages include timing diagnostics
- Results are deterministic given same inputs
