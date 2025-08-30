"""
Main orchestrator for running ingest operations across all sources.
"""
import argparse
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any

from ingest.common.ingest_engine import IngestEngine
from ingest.common.normalize_engine import NormalizeEngine


def setup_logging(verbose: bool = False) -> None:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )


def run_ingest_for_source(source_name: str, source_dir: Path, data_lake_root: Path, 
                         schema_path: Path, canonical_schema_path: Path) -> Dict[str, Any]:
    """Run ingest for a specific source."""
    logging.info(f"Processing source: {source_name}")
    
    if not source_dir.exists():
        logging.warning(f"Source directory does not exist: {source_dir}")
        return {
            "source": source_name,
            "status": "skipped",
            "reason": "Directory does not exist",
            "stats": None
        }
    
    try:
        # Initialize engines
        ingest_engine = IngestEngine(data_lake_root, schema_path)
        
        # Run ingest
        ingest_stats = ingest_engine.ingest_directory(source_dir, "*.jsonl")
        
        if not ingest_stats:
            return {
                "source": source_name,
                "status": "completed",
                "reason": "No files found",
                "stats": {"valid_records": 0, "rejected_records": 0}
            }
        
        # Aggregate stats
        total_valid = sum(stats["valid_records"] for stats in ingest_stats)
        total_rejected = sum(stats["rejected_records"] for stats in ingest_stats)
        
        return {
            "source": source_name,
            "status": "completed",
            "reason": "Success",
            "stats": {
                "valid_records": total_valid,
                "rejected_records": total_rejected,
                "files_processed": len(ingest_stats)
            }
        }
        
    except Exception as e:
        logging.error(f"Error processing source {source_name}: {e}")
        return {
            "source": source_name,
            "status": "failed",
            "reason": str(e),
            "stats": None
        }


def run_normalize(data_lake_root: Path, canonical_schema_path: Path) -> Dict[str, Any]:
    """Run normalization on all raw files."""
    logging.info("Starting normalization process")
    
    try:
        normalize_engine = NormalizeEngine(data_lake_root, canonical_schema_path)
        normalize_stats = normalize_engine.normalize_all_raw_files()
        
        if not normalize_stats:
            return {
                "status": "completed",
                "reason": "No raw files found",
                "stats": {"normalized_records": 0, "rejected_records": 0}
            }
        
        # Aggregate stats
        total_normalized = sum(stats["normalized_records"] for stats in normalize_stats)
        total_rejected = sum(stats["rejected_records"] for stats in normalize_stats)
        
        return {
            "status": "completed",
            "reason": "Success",
            "stats": {
                "normalized_records": total_normalized,
                "rejected_records": total_rejected,
                "files_processed": len(normalize_stats)
            }
        }
        
    except Exception as e:
        logging.error(f"Error during normalization: {e}")
        return {
            "status": "failed",
            "reason": str(e),
            "stats": None
        }


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Main orchestrator for ingest operations")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--sources-root", default="sources", help="Root directory containing source directories")
    parser.add_argument("--data-lake-root", default="data_lake", help="Data lake root directory")
    parser.add_argument("--schema-path", default="schemas/note_raw.schema.json", help="Path to raw schema file")
    parser.add_argument("--canonical-schema-path", default="schemas/note_canonical.schema.json", help="Path to canonical schema file")
    parser.add_argument("--normalize", action="store_true", help="Also run normalization after ingest")
    parser.add_argument("--sources", nargs="+", choices=["A", "B", "C"], help="Specific sources to process (default: all)")
    
    args = parser.parse_args()
    setup_logging(args.verbose)
    
    # Setup paths
    sources_root = Path(args.sources_root)
    data_lake_root = Path(args.data_lake_root)
    schema_path = Path(args.schema_path)
    canonical_schema_path = Path(args.canonical_schema_path)
    
    # Validate paths
    if not sources_root.exists():
        logging.error(f"Sources root directory does not exist: {sources_root}")
        return 1
    
    if not schema_path.exists():
        logging.error(f"Schema file does not exist: {schema_path}")
        return 1
    
    if not canonical_schema_path.exists():
        logging.error(f"Canonical schema file does not exist: {canonical_schema_path}")
        return 1
    
    # Determine which sources to process
    if args.sources:
        sources_to_process = args.sources
    else:
        sources_to_process = ["A", "B", "C"]
    
    logging.info(f"Processing sources: {', '.join(sources_to_process)}")
    
    # Process each source
    results = []
    for source_name in sources_to_process:
        source_dir = sources_root / source_name
        result = run_ingest_for_source(
            source_name, source_dir, data_lake_root, 
            schema_path, canonical_schema_path
        )
        results.append(result)
    
    # Print summary
    logging.info("=== Ingest Summary ===")
    total_valid = 0
    total_rejected = 0
    
    for result in results:
        status = result["status"]
        source = result["source"]
        reason = result["reason"]
        
        if status == "completed":
            stats = result["stats"]
            valid = stats.get("valid_records", 0)
            rejected = stats.get("rejected_records", 0)
            total_valid += valid
            total_rejected += rejected
            
            logging.info(f"{source}: {status} - {valid} valid, {rejected} rejected records")
        else:
            logging.warning(f"{source}: {status} - {reason}")
    
    logging.info(f"Total: {total_valid} valid, {total_rejected} rejected records")
    
    # Run normalization if requested
    if args.normalize:
        logging.info("=== Starting Normalization ===")
        norm_result = run_normalize(data_lake_root, canonical_schema_path)
        
        if norm_result["status"] == "completed":
            stats = norm_result["stats"]
            normalized = stats.get("normalized_records", 0)
            rejected = stats.get("rejected_records", 0)
            logging.info(f"Normalization: {norm_result['status']} - {normalized} normalized, {rejected} rejected records")
        else:
            logging.error(f"Normalization: {norm_result['status']} - {norm_result['reason']}")
    
    # Check for failures
    failed_sources = [r for r in results if r["status"] == "failed"]
    if failed_sources:
        logging.error(f"Failed sources: {[r['source'] for r in failed_sources]}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
