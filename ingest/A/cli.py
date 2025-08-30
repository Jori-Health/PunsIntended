"""
CLI for Source A connector ingest operations.
"""
import argparse
import logging
import sys
from pathlib import Path

# Add parent directory to path to import common modules
sys.path.append(str(Path(__file__).parent.parent.parent))

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


def ingest_command(args: argparse.Namespace) -> int:
    """Execute ingest command."""
    try:
        # Setup paths
        source_dir = Path(args.source_dir)
        data_lake_root = Path(args.data_lake_root)
        schema_path = Path(args.schema_path)
        canonical_schema_path = Path(args.canonical_schema_path)
        
        # Validate paths
        if not source_dir.exists():
            logging.error(f"Source directory does not exist: {source_dir}")
            return 1
        
        if not schema_path.exists():
            logging.error(f"Schema file does not exist: {schema_path}")
            return 1
        
        if not canonical_schema_path.exists():
            logging.error(f"Canonical schema file does not exist: {canonical_schema_path}")
            return 1
        
        # Initialize engines
        ingest_engine = IngestEngine(data_lake_root, schema_path)
        normalize_engine = NormalizeEngine(data_lake_root, canonical_schema_path)
        
        # Execute ingest
        logging.info(f"Starting ingest from {source_dir}")
        ingest_stats = ingest_engine.ingest_directory(source_dir, "*.jsonl")
        
        if not ingest_stats:
            logging.warning("No files were ingested")
            return 0
        
        # Print ingest summary
        total_valid = sum(stats["valid_records"] for stats in ingest_stats)
        total_rejected = sum(stats["rejected_records"] for stats in ingest_stats)
        logging.info(f"Ingest completed: {total_valid} valid records, {total_rejected} rejected records")
        
        # Execute normalization if requested
        if args.normalize:
            logging.info("Starting normalization")
            normalize_stats = normalize_engine.normalize_all_raw_files()
            
            if normalize_stats:
                total_normalized = sum(stats["normalized_records"] for stats in normalize_stats)
                total_norm_rejected = sum(stats["rejected_records"] for stats in normalize_stats)
                logging.info(f"Normalization completed: {total_normalized} normalized records, {total_norm_rejected} rejected records")
            else:
                logging.warning("No files were normalized")
        
        return 0
        
    except Exception as e:
        logging.error(f"Error during ingest: {e}")
        return 1


def main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Source A connector for ingest operations")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Ingest command
    ingest_parser = subparsers.add_parser("ingest", help="Ingest JSONL files from source directory")
    ingest_parser.add_argument("source_dir", help="Source directory containing JSONL files")
    ingest_parser.add_argument("--data-lake-root", default="data_lake", help="Data lake root directory")
    ingest_parser.add_argument("--schema-path", default="schemas/note_raw.schema.json", help="Path to raw schema file")
    ingest_parser.add_argument("--canonical-schema-path", default="schemas/note_canonical.schema.json", help="Path to canonical schema file")
    ingest_parser.add_argument("--normalize", action="store_true", help="Also run normalization after ingest")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    setup_logging(args.verbose)
    
    if args.command == "ingest":
        return ingest_command(args)
    else:
        logging.error(f"Unknown command: {args.command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
