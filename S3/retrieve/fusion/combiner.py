import yaml
import pathlib
from typing import List, Dict, Any, Tuple
from ..utils.io import normalize_scores


def load_config(config_path: str = "configs/retrieval.yaml") -> Dict[str, Any]:
    """Load retrieval configuration."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def fuse_scores(bm25_scores: List[float], dense_scores: List[float], 
                method: str = "weighted_sum", weights: Dict[str, float] = None) -> List[float]:
    """
    Fuse BM25 and dense scores using specified method.
    
    Args:
        bm25_scores: List of BM25 scores
        dense_scores: List of dense scores
        method: Fusion method ("weighted_sum" or "ranked_logit")
        weights: Dictionary with w_bm25 and w_dense weights
    
    Returns:
        List of fused scores
    """
    if len(bm25_scores) != len(dense_scores):
        raise ValueError("BM25 and dense scores must have same length")
    
    if not weights:
        weights = {"w_bm25": 0.5, "w_dense": 0.5}
    
    # Normalize scores to [0,1] range
    norm_bm25 = normalize_scores(bm25_scores)
    norm_dense = normalize_scores(dense_scores)
    
    if method == "weighted_sum":
        return [weights["w_bm25"] * b + weights["w_dense"] * d 
                for b, d in zip(norm_bm25, norm_dense)]
    
    elif method == "ranked_logit":
        # Simple ranked logit fusion (can be enhanced)
        fused = []
        for b, d in zip(norm_bm25, norm_dense):
            # Combine scores using logit-like transformation
            combined = weights["w_bm25"] * b + weights["w_dense"] * d
            fused.append(combined)
        return fused
    
    else:
        raise ValueError(f"Unknown fusion method: {method}")


def merge_candidates(bm25_results: List[Dict[str, Any]], 
                    dense_results: List[Dict[str, Any]], 
                    K: int = 200) -> List[Dict[str, Any]]:
    """
    Merge BM25 and dense candidates, removing duplicates and keeping top K.
    
    Args:
        bm25_results: List of BM25 results with chunk_id and score
        dense_results: List of dense results with chunk_id and score
        K: Maximum number of candidates to return
    
    Returns:
        List of merged candidates with both scores
    """
    # Create mapping from chunk_id to scores
    candidates = {}
    
    # Add BM25 results
    for i, result in enumerate(bm25_results):
        chunk_id = result['chunk_id']
        if chunk_id not in candidates:
            candidates[chunk_id] = {
                'chunk_id': chunk_id,
                's_bm25': result.get('score', 0.0),
                's_dense': 0.0,
                'source_id': result.get('source_id', ''),
                'note_uid': result.get('note_uid', '')
            }
    
    # Add dense results
    for i, result in enumerate(dense_results):
        chunk_id = result['chunk_id']
        if chunk_id in candidates:
            candidates[chunk_id]['s_dense'] = result.get('score', 0.0)
        else:
            candidates[chunk_id] = {
                'chunk_id': chunk_id,
                's_bm25': 0.0,
                's_dense': result.get('score', 0.0),
                'source_id': result.get('source_id', ''),
                'note_uid': result.get('note_uid', '')
            }
    
    # Convert to list and sort by fused score
    candidate_list = list(candidates.values())
    
    # Calculate fused scores
    bm25_scores = [c['s_bm25'] for c in candidate_list]
    dense_scores = [c['s_dense'] for c in candidate_list]
    fused_scores = fuse_scores(bm25_scores, dense_scores)
    
    # Add fused scores and sort
    for i, candidate in enumerate(candidate_list):
        candidate['fusion_score'] = fused_scores[i]
    
    # Sort by fused score (descending) and take top K
    candidate_list.sort(key=lambda x: x['fusion_score'], reverse=True)
    return candidate_list[:K]


def stub_bm25_search(query: str, chunks: Dict[str, Dict[str, Any]], 
                    k1: float = 0.9, b: float = 0.4, top_k: int = 200) -> List[Dict[str, Any]]:
    """
    Stub BM25 search implementation.
    In a real implementation, this would use an actual BM25 index.
    """
    results = []
    query_terms = query.lower().split()
    
    for chunk_id, chunk_data in chunks.items():
        text = chunk_data.get('text', '').lower()
        score = 0.0
        
        # Simple term frequency scoring
        for term in query_terms:
            if term in text:
                # Simple TF-IDF-like scoring
                tf = text.count(term)
                score += tf * 0.1  # Simplified scoring
        
        if score > 0:
            results.append({
                'chunk_id': chunk_id,
                'score': score,
                'source_id': chunk_data.get('source_id', ''),
                'note_uid': chunk_data.get('note_uid', '')
            })
    
    # Sort by score and return top_k
    results.sort(key=lambda x: x['score'], reverse=True)
    return results[:top_k]


def stub_dense_search(query: str, chunks: Dict[str, Dict[str, Any]], 
                     top_k: int = 200) -> List[Dict[str, Any]]:
    """
    Stub dense search implementation.
    In a real implementation, this would use an actual dense encoder and ANN index.
    """
    results = []
    query_terms = query.lower().split()
    
    for chunk_id, chunk_data in chunks.items():
        text = chunk_data.get('text', '').lower()
        score = 0.0
        
        # Simple semantic similarity scoring (stub)
        for term in query_terms:
            if term in text:
                # Simulate semantic similarity
                score += 0.2  # Base similarity score
                
                # Add some randomness for variety
                import random
                score += random.uniform(0, 0.1)
        
        if score > 0:
            results.append({
                'chunk_id': chunk_id,
                'score': score,
                'source_id': chunk_data.get('source_id', ''),
                'note_uid': chunk_data.get('note_uid', '')
            })
    
    # Sort by score and return top_k
    results.sort(key=lambda x: x['score'], reverse=True)
    return results[:top_k]
