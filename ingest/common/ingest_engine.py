"""
Core ingest engine for processing JSONL files and writing to data lake.
"""
import logging
from pathlib import Path
from typing import Dict, Any, List, Tuple
from datetime import datetime, UTC

from .schema_validator import validate_raw_note
from .provenance import calculate_checksum, create_lineage_info, write_lineage_file, enhance_provenance
from .io_utils import read_jsonl_file, write_jsonl_file, write_rejected_record, get_output_filename

logger = logging.getLogger(__name__)


class IngestEngine:
    """Engine for ingesting JSONL files into the data lake."""
    
    def __init__(self, data_lake_root: Path, schema_path: Path):
        """
        Initialize ingest engine.
        
        Args:
            data_lake_root: Root path to data lake directory
            schema_path: Path to raw note schema file
        """
        self.data_lake_root = Path(data_lake_root)
        self.schema_path = Path(schema_path)
        self.raw_dir = self.data_lake_root / "raw"
        self.rejected_dir = self.data_lake_root / "rejected"
        
        # Ensure directories exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.rejected_dir.mkdir(parents=True, exist_ok=True)
    
    def ingest_file(self, source_file: Path) -> Dict[str, Any]:
        """
        Ingest a single JSONL file.
        
        Args:
            source_file: Path to source JSONL file
            
        Returns:
            Dictionary with ingest statistics
        """
        logger.info(f"Starting ingest of {source_file}")
        
        stats = {
            "source_file": str(source_file),
            "total_records": 0,
            "valid_records": 0,
            "rejected_records": 0,
            "errors": []
        }
        
        valid_records = []
        ingest_timestamp = datetime.now(UTC).isoformat()
        
        try:
            # Read and process each line
            for line_num, record in read_jsonl_file(source_file):
                stats["total_records"] += 1
                
                # Skip records that already have JSON decode errors
                if "error" in record:
                    stats["rejected_records"] += 1
                    stats["errors"].append(f"Line {line_num}: {record['error']}")
                    continue
                
                # Calculate checksum
                record_str = str(record)
                checksum = calculate_checksum(record_str)
                
                # Validate against schema
                is_valid, error_msg = validate_raw_note(record, self.schema_path)
                
                if is_valid:
                    # Enhance with provenance
                    enhanced_record = enhance_provenance(record, source_file, checksum)
                    valid_records.append(enhanced_record)
                    stats["valid_records"] += 1
                else:
                    # Write to rejected
                    write_rejected_record(
                        record, 
                        f"Schema validation failed: {error_msg}", 
                        self.rejected_dir, 
                        source_file
                    )
                    stats["rejected_records"] += 1
                    stats["errors"].append(f"Line {line_num}: {error_msg}")
            
            # Write valid records to raw data lake
            if valid_records:
                output_filename = get_output_filename(source_file, "_raw")
                output_path = self.raw_dir / output_filename
                write_jsonl_file(valid_records, output_path)
                
                # Write lineage file
                lineage_data = create_lineage_info(source_file, checksum, ingest_timestamp)
                lineage_data.update({
                    "valid_records": len(valid_records),
                    "rejected_records": stats["rejected_records"],
                    "output_file": str(output_path)
                })
                write_lineage_file(self.raw_dir, source_file, lineage_data)
            
            logger.info(f"Completed ingest of {source_file}: {stats['valid_records']} valid, {stats['rejected_records']} rejected")
            
        except Exception as e:
            error_msg = f"Error during ingest of {source_file}: {e}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)
        
        return stats
    
    def ingest_directory(self, source_dir: Path, pattern: str = "*.jsonl") -> List[Dict[str, Any]]:
        """
        Ingest all JSONL files in a directory.
        
        Args:
            source_dir: Directory containing source files
            pattern: File pattern to match
            
        Returns:
            List of ingest statistics for each file
        """
        source_path = Path(source_dir)
        if not source_path.exists():
            logger.error(f"Source directory does not exist: {source_dir}")
            return []
        
        all_stats = []
        jsonl_files = list(source_path.glob(pattern))
        
        if not jsonl_files:
            logger.warning(f"No JSONL files found in {source_dir} matching pattern '{pattern}'")
            return []
        
        logger.info(f"Found {len(jsonl_files)} JSONL files to ingest")
        
        for file_path in jsonl_files:
            stats = self.ingest_file(file_path)
            all_stats.append(stats)
        
        return all_stats
