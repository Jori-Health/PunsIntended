"""
Normalize engine for transforming raw data into canonical format.
"""
import logging
import uuid
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime

from .schema_validator import validate_canonical_note
from .io_utils import read_jsonl_file, write_jsonl_file, write_rejected_record, get_output_filename

logger = logging.getLogger(__name__)


class NormalizeEngine:
    """Engine for normalizing raw data into canonical format."""
    
    def __init__(self, data_lake_root: Path, canonical_schema_path: Path):
        """
        Initialize normalize engine.
        
        Args:
            data_lake_root: Root path to data lake directory
            canonical_schema_path: Path to canonical note schema file
        """
        self.data_lake_root = Path(data_lake_root)
        self.canonical_schema_path = Path(canonical_schema_path)
        self.raw_dir = self.data_lake_root / "raw"
        self.standardized_dir = self.data_lake_root / "standardized"
        self.rejected_dir = self.data_lake_root / "rejected"
        
        # Ensure directories exist
        self.standardized_dir.mkdir(parents=True, exist_ok=True)
        self.rejected_dir.mkdir(parents=True, exist_ok=True)
    
    def transform_to_canonical(self, raw_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw record to canonical format.
        
        Args:
            raw_record: Raw note record
            
        Returns:
            Canonical note record
        """
        # Generate unique ID
        uid = str(uuid.uuid4())
        
        # Extract source_id from source field
        source_id = raw_record.get('source', 'unknown')
        
        # Transform to canonical format
        canonical_record = {
            "uid": uid,
            "patient_id": raw_record.get('patient_id'),
            "mrn": raw_record.get('mrn'),
            "source_id": source_id,
            "encounter": {
                "ts": raw_record.get('note_datetime'),
                "section": raw_record.get('section')
            },
            "content": {
                "raw_text": raw_record.get('text')
            },
            "provenance": {
                "source": raw_record.get('source'),
                "oncologist_id": raw_record.get('oncologist_id'),
                "oncologist_name": raw_record.get('oncologist_name'),
                "ingested_from": raw_record.get('provenance', {}).get('ingested_from'),
                "checksum": raw_record.get('provenance', {}).get('checksum')
            },
            "validation": {
                "status": "validated"
            }
        }
        
        return canonical_record
    
    def normalize_file(self, raw_file: Path) -> Dict[str, Any]:
        """
        Normalize a single raw JSONL file.
        
        Args:
            raw_file: Path to raw JSONL file
            
        Returns:
            Dictionary with normalization statistics
        """
        logger.info(f"Starting normalization of {raw_file}")
        
        stats = {
            "source_file": str(raw_file),
            "total_records": 0,
            "normalized_records": 0,
            "rejected_records": 0,
            "errors": []
        }
        
        normalized_records = []
        
        try:
            # Read and process each line
            for line_num, raw_record in read_jsonl_file(raw_file):
                stats["total_records"] += 1
                
                # Skip records that already have JSON decode errors
                if "error" in raw_record:
                    stats["rejected_records"] += 1
                    stats["errors"].append(f"Line {line_num}: {raw_record['error']}")
                    continue
                
                try:
                    # Transform to canonical format
                    canonical_record = self.transform_to_canonical(raw_record)
                    
                    # Validate canonical record
                    is_valid, error_msg = validate_canonical_note(canonical_record, self.canonical_schema_path)
                    
                    if is_valid:
                        normalized_records.append(canonical_record)
                        stats["normalized_records"] += 1
                    else:
                        # Write to rejected
                        write_rejected_record(
                            raw_record,
                            f"Canonical validation failed: {error_msg}",
                            self.rejected_dir,
                            raw_file
                        )
                        stats["rejected_records"] += 1
                        stats["errors"].append(f"Line {line_num}: {error_msg}")
                
                except Exception as e:
                    # Write to rejected
                    error_msg = f"Transformation error: {e}"
                    write_rejected_record(
                        raw_record,
                        error_msg,
                        self.rejected_dir,
                        raw_file
                    )
                    stats["rejected_records"] += 1
                    stats["errors"].append(f"Line {line_num}: {error_msg}")
            
            # Write normalized records to standardized data lake
            if normalized_records:
                output_filename = get_output_filename(raw_file, "_canonical")
                output_path = self.standardized_dir / output_filename
                write_jsonl_file(normalized_records, output_path)
            
            logger.info(f"Completed normalization of {raw_file}: {stats['normalized_records']} normalized, {stats['rejected_records']} rejected")
            
        except Exception as e:
            error_msg = f"Error during normalization of {raw_file}: {e}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)
        
        return stats
    
    def normalize_all_raw_files(self) -> List[Dict[str, Any]]:
        """
        Normalize all raw files in the data lake.
        
        Returns:
            List of normalization statistics for each file
        """
        if not self.raw_dir.exists():
            logger.error(f"Raw directory does not exist: {self.raw_dir}")
            return []
        
        all_stats = []
        raw_files = list(self.raw_dir.glob("*_raw_*.jsonl"))
        
        if not raw_files:
            logger.warning(f"No raw files found in {self.raw_dir}")
            return []
        
        logger.info(f"Found {len(raw_files)} raw files to normalize")
        
        for file_path in raw_files:
            stats = self.normalize_file(file_path)
            all_stats.append(stats)
        
        return all_stats
