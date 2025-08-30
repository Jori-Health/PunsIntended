"""
IO utilities for reading and writing data during ingest operations.
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Iterator, Tuple
from datetime import datetime, UTC

logger = logging.getLogger(__name__)


def read_jsonl_file(file_path: Path) -> Iterator[Tuple[int, Dict[str, Any]]]:
    """
    Read JSONL file and yield (line_number, data) tuples.
    
    Yields:
        Tuple of (line_number, parsed_json_data)
    """
    try:
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    yield line_num, data
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error at line {line_num} in {file_path}: {e}")
                    yield line_num, {"error": f"JSON decode error: {e}", "raw_line": line}
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        raise


def write_jsonl_file(data_list: List[Dict[str, Any]], output_path: Path) -> None:
    """Write list of dictionaries to JSONL file."""
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            for data in data_list:
                f.write(json.dumps(data) + '\n')
        logger.info(f"Wrote {len(data_list)} records to {output_path}")
    except Exception as e:
        logger.error(f"Error writing to {output_path}: {e}")
        raise


def write_rejected_record(record: Dict[str, Any], error_reason: str, output_dir: Path, source_file: Path) -> None:
    """Write rejected record to rejected directory with error information."""
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create rejected record with error info
        rejected_record = {
            "original_record": record,
            "error_reason": error_reason,
            "source_file": str(source_file),
            "rejected_at": datetime.now(UTC).isoformat()
        }
        
        # Generate filename based on source and timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{source_file.stem}_rejected_{timestamp}.jsonl"
        output_path = output_dir / filename
        
        write_jsonl_file([rejected_record], output_path)
        logger.warning(f"Rejected record from {source_file}: {error_reason}")
        
    except Exception as e:
        logger.error(f"Error writing rejected record: {e}")
        raise


def get_output_filename(source_file: Path, suffix: str = "") -> str:
    """Generate output filename based on source file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = source_file.stem
    return f"{base_name}{suffix}_{timestamp}.jsonl"
