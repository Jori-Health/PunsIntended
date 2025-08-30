import json
import pathlib
from typing import Dict, List, Optional, Any, Iterator
import time


def load_jsonl(file_path: str) -> List[Dict[str, Any]]:
    """Load data from a JSONL file."""
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))
    return data


def save_jsonl(data: List[Dict[str, Any]], file_path: str) -> None:
    """Save data to a JSONL file."""
    with open(file_path, 'w', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')


def load_chunks(chunks_path: str) -> Dict[str, Dict[str, Any]]:
    """Load chunks from JSONL and create a mapping from chunk_id to chunk data."""
    chunks = {}
    if pathlib.Path(chunks_path).is_file():
        # Single file
        chunk_list = load_jsonl(chunks_path)
        for chunk in chunk_list:
            chunks[chunk['chunk_id']] = chunk
    else:
        # Directory - look for chunks.jsonl files
        chunks_dir = pathlib.Path(chunks_path)
        for chunks_file in chunks_dir.rglob("chunks.jsonl"):
            chunk_list = load_jsonl(str(chunks_file))
            for chunk in chunk_list:
                chunks[chunk['chunk_id']] = chunk
    return chunks


def get_chunk_text(chunk_id: str, chunks: Dict[str, Dict[str, Any]]) -> str:
    """Get text content for a given chunk_id."""
    if chunk_id not in chunks:
        raise KeyError(f"Chunk ID {chunk_id} not found in chunks")
    return chunks[chunk_id].get('text', '')


def get_chunk_source(chunk_id: str, chunks: Dict[str, Dict[str, Any]]) -> str:
    """Get source information for a given chunk_id."""
    if chunk_id not in chunks:
        raise KeyError(f"Chunk ID {chunk_id} not found in chunks")
    return chunks[chunk_id].get('source_id', '')


def load_note_links(links_path: Optional[str]) -> Dict[str, str]:
    """Load note links to map chunk_id to patient_uid."""
    if not links_path:
        return {}
    
    links = {}
    try:
        links_data = load_jsonl(links_path)
        for link in links_data:
            chunk_id = link.get('chunk_id')
            patient_uid = link.get('patient_uid')
            if chunk_id and patient_uid:
                links[chunk_id] = patient_uid
    except Exception:
        # If links file doesn't exist or is malformed, return empty dict
        pass
    return links


def normalize_scores(scores: List[float]) -> List[float]:
    """Normalize scores to [0,1] range using min-max normalization."""
    if not scores:
        return []
    
    min_score = min(scores)
    max_score = max(scores)
    
    if max_score == min_score:
        return [0.5] * len(scores)  # All same score -> 0.5
    
    return [(score - min_score) / (max_score - min_score) for score in scores]


def timer(func):
    """Decorator to measure function execution time."""
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        return result
    return wrapper

