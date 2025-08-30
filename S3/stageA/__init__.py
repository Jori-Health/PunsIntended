import sys
import json
import pathlib
import argparse
from typing import Dict, List, Any
import time

# Add parent directory to path for imports
sys.path.append(str(pathlib.Path(__file__).parent.parent))

from retrieve.utils.io import load_chunks, save_jsonl, timer
from retrieve.fusion.combiner import (
    load_config, stub_bm25_search, stub_dense_search, merge_candidates
)


@timer
def run_stage_a(bm25_dir: str, dense_dir: str, chunks_path: str, 
                query: str, out_dir: str) -> Dict[str, Any]:
    """
    Run Stage A: Scouts (BM25 + Dense search and fusion)
    
    Args:
        bm25_dir: Directory containing BM25 artifacts
        dense_dir: Directory containing dense artifacts  
        chunks_path: Path to chunks JSONL file
        query: Search query
        out_dir: Output directory for results
    
    Returns:
        Dictionary with diagnostics
    """
    start_time = time.perf_counter()
    
    # Load configuration
    config = load_config()
    K_A = config.get('K_A', 200)
    bm25_config = config.get('bm25', {})
    dense_config = config.get('dense', {})
    fusion_config = config.get('fusion', {})
    
    # Load chunks
    chunks = load_chunks(chunks_path)
    
    # Run BM25 search
    bm25_start = time.perf_counter()
    bm25_results = stub_bm25_search(
        query, chunks, 
        k1=bm25_config.get('k1', 0.9),
        b=bm25_config.get('b', 0.4),
        top_k=K_A
    )
    bm25_time = time.perf_counter() - bm25_start
    
    # Run dense search
    dense_start = time.perf_counter()
    dense_results = stub_dense_search(
        query, chunks, top_k=K_A
    )
    dense_time = time.perf_counter() - dense_start
    
    # Merge and fuse candidates
    fusion_start = time.perf_counter()
    candidates = merge_candidates(
        bm25_results, dense_results, K=K_A
    )
    fusion_time = time.perf_counter() - fusion_start
    
    # Create output directory
    out_path = pathlib.Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    
    # Save candidates
    candidates_file = out_path / "candidates.jsonl"
    save_jsonl(candidates, str(candidates_file))
    
    # Save diagnostics
    diagnostics = {
        "stage": "A",
        "query": query,
        "total_chunks": len(chunks),
        "bm25_results": len(bm25_results),
        "dense_results": len(dense_results),
        "final_candidates": len(candidates),
        "K_A": K_A,
        "timing": {
            "bm25_search": bm25_time,
            "dense_search": dense_time,
            "fusion": fusion_time,
            "total": time.perf_counter() - start_time
        }
    }
    
    diagnostics_file = out_path / "diagnostics.json"
    with open(diagnostics_file, 'w') as f:
        json.dump(diagnostics, f, indent=2)
    
    return diagnostics


def main():
    parser = argparse.ArgumentParser(description="Stage A: Scouts (BM25 + Dense search)")
    parser.add_argument("run", help="Run stage A")
    parser.add_argument("bm25_dir", help="Directory containing BM25 artifacts")
    parser.add_argument("dense_dir", help="Directory containing dense artifacts")
    parser.add_argument("chunks_jsonl", help="Path to chunks JSONL file")
    parser.add_argument("query", help="Search query")
    parser.add_argument("out_dir", help="Output directory")
    
    args = parser.parse_args()
    
    if args.run != "run":
        print("Usage: python -m stageA run <bm25_dir> <dense_dir> <chunks_jsonl> \"<query>\" <out_dir>")
        sys.exit(1)
    
    try:
        diagnostics = run_stage_a(
            args.bm25_dir, args.dense_dir, args.chunks_jsonl, 
            args.query, args.out_dir
        )
        print(f"Stage A completed successfully. Found {diagnostics['final_candidates']} candidates.")
        print(f"Total time: {diagnostics['timing']['total']:.3f}s")
        
    except Exception as e:
        print(f"Error in Stage A: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

