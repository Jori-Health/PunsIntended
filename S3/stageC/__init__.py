import sys
import json
import pathlib
import argparse
from typing import Dict, List, Any, Optional
import time
import random

# Add parent directory to path for imports
sys.path.append(str(pathlib.Path(__file__).parent.parent))

from retrieve.utils.io import load_jsonl, save_jsonl, load_chunks, load_note_links, timer
from retrieve.fusion.combiner import load_config


def stub_cross_encoder_scoring(query: str, chunk_text: str) -> float:
    """
    Stub cross-encoder scoring implementation.
    In a real implementation, this would use a cross-encoder model.
    
    Args:
        query: Search query
        chunk_text: Text content of the chunk
    
    Returns:
        Cross-encoder score
    """
    query_terms = query.lower().split()
    chunk_words = chunk_text.lower().split()
    
    # Simple cross-encoder scoring (stub)
    score = 0.0
    
    # Exact term matches
    for term in query_terms:
        if term in chunk_text.lower():
            score += 0.4
    
    # Semantic similarity (simulated)
    semantic_score = 0.0
    for term in query_terms:
        for word in chunk_words:
            if len(term) > 2 and len(word) > 2:
                # Simple character overlap
                overlap = len(set(term) & set(word)) / len(set(term) | set(word))
                semantic_score += overlap * 0.1
    
    score += semantic_score
    
    # Add some randomness for variety
    score += random.uniform(0, 0.1)
    
    # Normalize to [0, 1]
    return min(score, 1.0)


def calibrate_scores(scores: List[float], method: str = "isotonic") -> List[float]:
    """
    Calibrate scores using specified method.
    
    Args:
        scores: List of raw scores
        method: Calibration method ("isotonic", "platt", or "minmax")
    
    Returns:
        List of calibrated scores
    """
    if not scores:
        return []
    
    if method == "minmax":
        # Simple min-max normalization
        min_score = min(scores)
        max_score = max(scores)
        if max_score == min_score:
            return [0.5] * len(scores)
        return [(score - min_score) / (max_score - min_score) for score in scores]
    
    elif method == "isotonic":
        # Stub isotonic regression (in real implementation, use sklearn.isotonic)
        # For now, use min-max as approximation
        return calibrate_scores(scores, "minmax")
    
    elif method == "platt":
        # Stub Platt scaling (in real implementation, use logistic regression)
        # For now, use min-max as approximation
        return calibrate_scores(scores, "minmax")
    
    else:
        raise ValueError(f"Unknown calibration method: {method}")


@timer
def run_stage_c(rescored_path: str, chunks_path: str, out_dir: str, 
                links_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Run Stage C: Judges (Cross-encoder scoring and calibration)
    
    Args:
        rescored_path: Path to rescored.jsonl from Stage B
        chunks_path: Path to chunks JSONL file
        out_dir: Output directory for results
        links_path: Optional path to note_links.jsonl for patient_uid attachment
    
    Returns:
        Dictionary with diagnostics
    """
    start_time = time.perf_counter()
    
    # Load configuration
    config = load_config()
    K_C = config.get('K_C', 10)
    xenc_config = config.get('xenc', {})
    
    # Load rescored candidates from Stage B
    rescored = load_jsonl(rescored_path)
    
    # Load chunks for text lookup
    chunks = load_chunks(chunks_path)
    
    # Load note links if provided
    note_links = load_note_links(links_path)
    
    # Apply cross-encoder scoring
    final_results = []
    
    for candidate in rescored[:K_C]:  # Process top K_C candidates
        chunk_id = candidate['chunk_id']
        
        # Get chunk text
        try:
            chunk_text = chunks[chunk_id].get('text', '')
        except KeyError:
            # Skip if chunk not found
            continue
        
        # Apply cross-encoder scoring
        xenc_score = stub_cross_encoder_scoring(
            query="",  # Query not needed for this stage
            chunk_text=chunk_text
        )
        
        # Get chunk metadata
        chunk_data = chunks[chunk_id]
        
        # Create final result
        final_result = {
            'chunk_id': chunk_id,
            'calibrated_score': xenc_score,  # Will be calibrated later
            'raw_xenc_score': xenc_score,
            's_li': candidate.get('s_li', 0.0),
            'fusion_score': candidate.get('fusion_score', 0.0),
            'source_id': candidate.get('source_id', ''),
            'pointers': {
                'chunk_offset': chunk_data.get('offset', 0),
                'file': chunk_data.get('source_file', '')
            }
        }
        
        # Add patient_uid if available
        if chunk_id in note_links:
            final_result['patient_uid'] = note_links[chunk_id]
        
        final_results.append(final_result)
    
    # Calibrate scores
    raw_scores = [r['raw_xenc_score'] for r in final_results]
    calibrated_scores = calibrate_scores(raw_scores, method="minmax")
    
    # Update with calibrated scores
    for i, result in enumerate(final_results):
        result['calibrated_score'] = calibrated_scores[i]
    
    # Sort by calibrated score (descending)
    final_results.sort(key=lambda x: x['calibrated_score'], reverse=True)
    
    # Take top K_C
    final_results = final_results[:K_C]
    
    # Create output directory
    out_path = pathlib.Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    
    # Save final results
    final_file = out_path / "final.jsonl"
    save_jsonl(final_results, str(final_file))
    
    # Save diagnostics
    diagnostics = {
        "stage": "C",
        "input_candidates": len(rescored),
        "processed_candidates": len(final_results),
        "final_results": len(final_results),
        "K_C": K_C,
        "patient_uid_attached": sum(1 for r in final_results if 'patient_uid' in r),
        "timing": {
            "cross_encoding": time.perf_counter() - start_time,
            "total": time.perf_counter() - start_time
        }
    }
    
    diagnostics_file = out_path / "diagnostics.json"
    with open(diagnostics_file, 'w') as f:
        json.dump(diagnostics, f, indent=2)
    
    return diagnostics


def main():
    parser = argparse.ArgumentParser(description="Stage C: Judges (Cross-encoder scoring)")
    parser.add_argument("run", help="Run stage C")
    parser.add_argument("rescored_jsonl", help="Path to rescored.jsonl from Stage B")
    parser.add_argument("chunks_jsonl", help="Path to chunks JSONL file")
    parser.add_argument("out_dir", help="Output directory")
    parser.add_argument("--links", help="Optional path to note_links.jsonl")
    
    args = parser.parse_args()
    
    if args.run != "run":
        print("Usage: python -m stageC run <rescored.jsonl> <chunks_jsonl> <out_dir> [--links <note_links.jsonl>]")
        sys.exit(1)
    
    try:
        diagnostics = run_stage_c(
            args.rescored_jsonl, args.chunks_jsonl, args.out_dir, args.links
        )
        print(f"Stage C completed successfully. Final results: {diagnostics['final_results']} candidates.")
        print(f"Patient UIDs attached: {diagnostics['patient_uid_attached']}")
        print(f"Total time: {diagnostics['timing']['total']:.3f}s")
        
    except Exception as e:
        print(f"Error in Stage C: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

