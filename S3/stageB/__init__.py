import sys
import json
import pathlib
import argparse
from typing import Dict, List, Any
import time
import random

# Add parent directory to path for imports
sys.path.append(str(pathlib.Path(__file__).parent.parent))

from retrieve.utils.io import load_jsonl, save_jsonl, load_chunks, timer
from retrieve.fusion.combiner import load_config


def stub_late_interaction_scoring(query: str, chunk_text: str, 
                                 max_len: int = 512) -> Dict[str, Any]:
    """
    Stub late interaction scoring implementation.
    In a real implementation, this would use ColBERT or similar model.
    
    Args:
        query: Search query
        chunk_text: Text content of the chunk
        max_len: Maximum sequence length
    
    Returns:
        Dictionary with score and optional evidence
    """
    query_terms = query.lower().split()
    chunk_words = chunk_text.lower().split()
    
    # Simple late interaction scoring (stub)
    score = 0.0
    evidence = []
    
    for i, term in enumerate(query_terms):
        term_score = 0.0
        term_evidence = []
        
        for j, word in enumerate(chunk_words):
            if term in word or word in term:
                # Simulate token-level interaction
                interaction_score = 0.3 + random.uniform(0, 0.2)
                term_score += interaction_score
                
                # Add evidence (optional)
                if random.random() < 0.3:  # 30% chance to add evidence
                    term_evidence.append({
                        "token": word,
                        "weight": interaction_score,
                        "pos": j
                    })
        
        score += term_score
        
        if term_evidence:
            evidence.extend(term_evidence)
    
    # Normalize score
    if len(query_terms) > 0:
        score = min(score / len(query_terms), 1.0)
    else:
        score = 0.0
    
    result = {
        "s_li": score
    }
    
    # Add evidence if available
    if evidence:
        result["evidence"] = evidence[:10]  # Limit evidence items
    
    return result


@timer
def run_stage_b(candidates_path: str, chunks_path: str, out_dir: str) -> Dict[str, Any]:
    """
    Run Stage B: Inspectors (Late-interaction rescoring)
    
    Args:
        candidates_path: Path to candidates.jsonl from Stage A
        chunks_path: Path to chunks JSONL file
        out_dir: Output directory for results
    
    Returns:
        Dictionary with diagnostics
    """
    start_time = time.perf_counter()
    
    # Load configuration
    config = load_config()
    K_B = config.get('K_B', 50)
    li_config = config.get('li', {})
    
    # Load candidates from Stage A
    candidates = load_jsonl(candidates_path)
    
    # Load chunks for text lookup
    chunks = load_chunks(chunks_path)
    
    # Rescore candidates using late interaction
    rescored = []
    max_len = li_config.get('max_len', 512)
    
    for candidate in candidates[:K_B]:  # Process top K_B candidates
        chunk_id = candidate['chunk_id']
        
        # Get chunk text
        try:
            chunk_text = chunks[chunk_id].get('text', '')
        except KeyError:
            # Skip if chunk not found
            continue
        
        # Apply late interaction scoring
        li_result = stub_late_interaction_scoring(
            query="progression after FOLFIRINOX",  # Use a default query for scoring
            chunk_text=chunk_text,
            max_len=max_len
        )
        
        # Combine with original candidate data
        rescored_candidate = {
            'chunk_id': chunk_id,
            's_li': li_result['s_li'],
            'fusion_score': candidate.get('fusion_score', 0.0),
            's_bm25': candidate.get('s_bm25', 0.0),
            's_dense': candidate.get('s_dense', 0.0),
            'source_id': candidate.get('source_id', ''),
            'note_uid': candidate.get('note_uid', '')
        }
        
        # Add evidence if available
        if 'evidence' in li_result:
            rescored_candidate['evidence'] = li_result['evidence']
        
        rescored.append(rescored_candidate)
    
    # Sort by late interaction score (descending)
    rescored.sort(key=lambda x: x['s_li'], reverse=True)
    
    # Take top K_B
    rescored = rescored[:K_B]
    
    # Create output directory
    out_path = pathlib.Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    
    # Save rescored results
    rescored_file = out_path / "rescored.jsonl"
    save_jsonl(rescored, str(rescored_file))
    
    # Save diagnostics
    diagnostics = {
        "stage": "B",
        "input_candidates": len(candidates),
        "processed_candidates": len(rescored),
        "K_B": K_B,
        "timing": {
            "rescoring": time.perf_counter() - start_time,
            "total": time.perf_counter() - start_time
        }
    }
    
    diagnostics_file = out_path / "diagnostics.json"
    with open(diagnostics_file, 'w') as f:
        json.dump(diagnostics, f, indent=2)
    
    return diagnostics


def main():
    parser = argparse.ArgumentParser(description="Stage B: Inspectors (Late-interaction rescoring)")
    parser.add_argument("run", help="Run stage B")
    parser.add_argument("candidates_jsonl", help="Path to candidates.jsonl from Stage A")
    parser.add_argument("chunks_jsonl", help="Path to chunks JSONL file")
    parser.add_argument("out_dir", help="Output directory")
    
    args = parser.parse_args()
    
    if args.run != "run":
        print("Usage: python -m stageB run <candidates.jsonl> <chunks_jsonl> <out_dir>")
        sys.exit(1)
    
    try:
        diagnostics = run_stage_b(
            args.candidates_jsonl, args.chunks_jsonl, args.out_dir
        )
        print(f"Stage B completed successfully. Rescored {diagnostics['processed_candidates']} candidates.")
        print(f"Total time: {diagnostics['timing']['total']:.3f}s")
        
    except Exception as e:
        print(f"Error in Stage B: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

