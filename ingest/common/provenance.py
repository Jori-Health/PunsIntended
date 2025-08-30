"""
Provenance tracking utilities for ingest operations.
"""
import hashlib
import json
import logging
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)


def calculate_checksum(data: str) -> str:
    """Calculate SHA-256 checksum of data."""
    return hashlib.sha256(data.encode('utf-8')).hexdigest()


def create_lineage_info(source_file: Path, checksum: str, ingest_timestamp: str) -> Dict[str, Any]:
    """Create lineage information for ingested data."""
    return {
        "source_file": str(source_file),
        "checksum": checksum,
        "ingest_timestamp": ingest_timestamp,
        "ingest_version": "1.0"
    }


def write_lineage_file(output_dir: Path, source_file: Path, lineage_data: Dict[str, Any]) -> Path:
    """Write lineage information to JSON file."""
    lineage_file = output_dir / f"{source_file.stem}_lineage.json"
    try:
        with open(lineage_file, 'w') as f:
            json.dump(lineage_data, f, indent=2)
        logger.info(f"Wrote lineage file: {lineage_file}")
        return lineage_file
    except Exception as e:
        logger.error(f"Failed to write lineage file {lineage_file}: {e}")
        raise


def enhance_provenance(note_data: Dict[str, Any], source_file: Path, checksum: str) -> Dict[str, Any]:
    """Enhance note data with provenance information."""
    enhanced_data = note_data.copy()
    
    # Ensure provenance field exists
    if 'provenance' not in enhanced_data:
        enhanced_data['provenance'] = {}
    
    # Update provenance with additional info
    enhanced_data['provenance'].update({
        'ingested_from': str(source_file),
        'checksum': checksum,
        'ingest_timestamp': datetime.now(UTC).isoformat()
    })
    
    return enhanced_data
