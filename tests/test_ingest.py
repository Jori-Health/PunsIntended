"""
Tests for ingest and normalize functionality.
"""
import json
import tempfile
import shutil
from pathlib import Path
import pytest
import sys

# Add parent directory to path to import modules
sys.path.append(str(Path(__file__).parent.parent))

from ingest.common.ingest_engine import IngestEngine
from ingest.common.normalize_engine import NormalizeEngine
from ingest.common.schema_validator import validate_raw_note, validate_canonical_note
from ingest.common.provenance import calculate_checksum, enhance_provenance
from ingest.common.io_utils import read_jsonl_file, write_jsonl_file


class TestSchemaValidation:
    """Test schema validation functionality."""
    
    def test_valid_raw_note(self):
        """Test validation of valid raw note."""
        schema_path = Path("schemas/note_raw.schema.json")
        valid_note = {
            "patient_id": "P001",
            "mrn": "MDA-1001",
            "source": "Source-A",
            "oncologist_id": "ONC-A",
            "oncologist_name": "Dr. Alvarez",
            "note_datetime": "2024-10-27T16:45:00",
            "section": "Labs",
            "text": "Test note text",
            "provenance": {
                "ingested_from": "/sources/A/P001.txt",
                "checksum": "0x72ff5d2a386ecbe0"
            }
        }
        
        is_valid, error = validate_raw_note(valid_note, schema_path)
        assert is_valid
        assert error is None
    
    def test_invalid_raw_note_missing_required(self):
        """Test validation of invalid raw note with missing required fields."""
        schema_path = Path("schemas/note_raw.schema.json")
        invalid_note = {
            "patient_id": "P001",
            "mrn": "MDA-1001",
            # Missing required fields
        }
        
        is_valid, error = validate_raw_note(invalid_note, schema_path)
        assert not is_valid
        assert error is not None
    
    def test_valid_canonical_note(self):
        """Test validation of valid canonical note."""
        schema_path = Path("schemas/note_canonical.schema.json")
        valid_note = {
            "uid": "123e4567-e89b-12d3-a456-426614174000",
            "patient_id": "P001",
            "mrn": "MDA-1001",
            "source_id": "Source-A",
            "encounter": {
                "ts": "2024-10-27T16:45:00",
                "section": "Labs"
            },
            "content": {
                "raw_text": "Test note text"
            },
            "provenance": {
                "source": "Source-A",
                "oncologist_id": "ONC-A",
                "oncologist_name": "Dr. Alvarez",
                "ingested_from": "/sources/A/P001.txt",
                "checksum": "0x72ff5d2a386ecbe0"
            },
            "validation": {
                "status": "validated"
            }
        }
        
        is_valid, error = validate_canonical_note(valid_note, schema_path)
        assert is_valid
        assert error is None


class TestProvenance:
    """Test provenance functionality."""
    
    def test_calculate_checksum(self):
        """Test checksum calculation."""
        data = "test data"
        checksum = calculate_checksum(data)
        assert len(checksum) == 64  # SHA-256 hex length
        assert checksum.isalnum()
    
    def test_enhance_provenance(self):
        """Test provenance enhancement."""
        original_note = {
            "patient_id": "P001",
            "text": "Test note"
        }
        
        source_file = Path("/sources/A/test.jsonl")
        checksum = "0x1234567890abcdef"
        
        enhanced = enhance_provenance(original_note, source_file, checksum)
        
        assert enhanced["provenance"]["ingested_from"] == str(source_file)
        assert enhanced["provenance"]["checksum"] == checksum
        assert "ingest_timestamp" in enhanced["provenance"]


class TestIOUtils:
    """Test IO utilities."""
    
    def test_write_and_read_jsonl(self, tmp_path):
        """Test writing and reading JSONL files."""
        test_data = [
            {"id": 1, "text": "first"},
            {"id": 2, "text": "second"}
        ]
        
        output_file = tmp_path / "test.jsonl"
        write_jsonl_file(test_data, output_file)
        
        # Read back
        records = list(read_jsonl_file(output_file))
        assert len(records) == 2
        
        # Check line numbers and data
        assert records[0][0] == 1  # line number
        assert records[0][1]["id"] == 1
        assert records[1][0] == 2  # line number
        assert records[1][1]["id"] == 2


class TestIngestEngine:
    """Test ingest engine functionality."""
    
    @pytest.fixture
    def temp_data_lake(self, tmp_path):
        """Create temporary data lake structure."""
        data_lake = tmp_path / "data_lake"
        data_lake.mkdir()
        (data_lake / "raw").mkdir()
        (data_lake / "rejected").mkdir()
        return data_lake
    
    @pytest.fixture
    def sample_jsonl_file(self, tmp_path):
        """Create a sample JSONL file for testing."""
        sample_data = [
            {
                "patient_id": "P001",
                "mrn": "MDA-1001",
                "source": "Source-A",
                "oncologist_id": "ONC-A",
                "oncologist_name": "Dr. Alvarez",
                "note_datetime": "2024-10-27T16:45:00",
                "section": "Labs",
                "text": "Test note text",
                "provenance": {
                    "ingested_from": "/sources/A/P001.txt",
                    "checksum": "0x72ff5d2a386ecbe0"
                }
            },
            {
                "patient_id": "P002",
                "mrn": "MDA-1002",
                "source": "Source-A",
                "oncologist_id": "ONC-A",
                "oncologist_name": "Dr. Alvarez",
                "note_datetime": "2024-10-28T16:45:00",
                "section": "Treatment",
                "text": "Another test note",
                "provenance": {
                    "ingested_from": "/sources/A/P002.txt",
                    "checksum": "0x1234567890abcdef"
                }
            }
        ]
        
        jsonl_file = tmp_path / "test.jsonl"
        write_jsonl_file(sample_data, jsonl_file)
        return jsonl_file
    
    def test_ingest_valid_file(self, temp_data_lake, sample_jsonl_file):
        """Test ingesting a valid JSONL file."""
        schema_path = Path("schemas/note_raw.schema.json")
        engine = IngestEngine(temp_data_lake, schema_path)
        
        stats = engine.ingest_file(sample_jsonl_file)
        
        assert stats["valid_records"] == 2
        assert stats["rejected_records"] == 0
        assert stats["total_records"] == 2
        
        # Check that files were created
        raw_files = list((temp_data_lake / "raw").glob("*.jsonl"))
        assert len(raw_files) == 1
        
        lineage_files = list((temp_data_lake / "raw").glob("*.json"))
        assert len(lineage_files) == 1
    
    def test_ingest_invalid_file(self, temp_data_lake, tmp_path):
        """Test ingesting a file with invalid records."""
        # Create file with invalid record
        invalid_data = [
            {
                "patient_id": "P001",
                # Missing required fields
                "text": "Invalid note"
            }
        ]
        
        jsonl_file = tmp_path / "invalid.jsonl"
        write_jsonl_file(invalid_data, jsonl_file)
        
        schema_path = Path("schemas/note_raw.schema.json")
        engine = IngestEngine(temp_data_lake, schema_path)
        
        stats = engine.ingest_file(jsonl_file)
        
        assert stats["valid_records"] == 0
        assert stats["rejected_records"] == 1
        assert stats["total_records"] == 1
        
        # Check that rejected file was created
        rejected_files = list((temp_data_lake / "rejected").glob("*.jsonl"))
        assert len(rejected_files) == 1


class TestNormalizeEngine:
    """Test normalize engine functionality."""
    
    @pytest.fixture
    def temp_data_lake_with_raw(self, tmp_path):
        """Create temporary data lake with raw files."""
        data_lake = tmp_path / "data_lake"
        data_lake.mkdir()
        (data_lake / "raw").mkdir()
        (data_lake / "standardized").mkdir()
        (data_lake / "rejected").mkdir()
        
        # Create a raw file
        raw_data = [
            {
                "patient_id": "P001",
                "mrn": "MDA-1001",
                "source": "Source-A",
                "oncologist_id": "ONC-A",
                "oncologist_name": "Dr. Alvarez",
                "note_datetime": "2024-10-27T16:45:00",
                "section": "Labs",
                "text": "Test note text",
                "provenance": {
                    "ingested_from": "/sources/A/P001.txt",
                    "checksum": "0x72ff5d2a386ecbe0"
                }
            }
        ]
        
        raw_file = data_lake / "raw" / "test_raw_20241201_120000.jsonl"
        write_jsonl_file(raw_data, raw_file)
        
        return data_lake
    
    def test_transform_to_canonical(self, temp_data_lake_with_raw):
        """Test transformation to canonical format."""
        canonical_schema_path = Path("schemas/note_canonical.schema.json")
        engine = NormalizeEngine(temp_data_lake_with_raw, canonical_schema_path)
        
        raw_record = {
            "patient_id": "P001",
            "mrn": "MDA-1001",
            "source": "Source-A",
            "oncologist_id": "ONC-A",
            "oncologist_name": "Dr. Alvarez",
            "note_datetime": "2024-10-27T16:45:00",
            "section": "Labs",
            "text": "Test note text",
            "provenance": {
                "ingested_from": "/sources/A/P001.txt",
                "checksum": "0x72ff5d2a386ecbe0"
            }
        }
        
        canonical = engine.transform_to_canonical(raw_record)
        
        assert "uid" in canonical
        assert canonical["patient_id"] == "P001"
        assert canonical["source_id"] == "Source-A"
        assert canonical["encounter"]["ts"] == "2024-10-27T16:45:00"
        assert canonical["content"]["raw_text"] == "Test note text"
        assert canonical["validation"]["status"] == "validated"
    
    def test_normalize_all_raw_files(self, temp_data_lake_with_raw):
        """Test normalizing all raw files."""
        canonical_schema_path = Path("schemas/note_canonical.schema.json")
        engine = NormalizeEngine(temp_data_lake_with_raw, canonical_schema_path)
        
        stats_list = engine.normalize_all_raw_files()
        
        assert len(stats_list) == 1
        stats = stats_list[0]
        
        assert stats["normalized_records"] == 1
        assert stats["rejected_records"] == 0
        
        # Check that canonical file was created
        canonical_files = list((temp_data_lake_with_raw / "standardized").glob("*.jsonl"))
        assert len(canonical_files) == 1


class TestIntegration:
    """Integration tests using test fixtures."""
    
    @pytest.fixture
    def temp_data_lake(self, tmp_path):
        """Create temporary data lake structure."""
        data_lake = tmp_path / "data_lake"
        data_lake.mkdir()
        (data_lake / "raw").mkdir()
        (data_lake / "standardized").mkdir()
        (data_lake / "rejected").mkdir()
        return data_lake
    
    def test_ingest_fixture_files(self, temp_data_lake):
        """Test ingesting the provided fixture files."""
        schema_path = Path("schemas/note_raw.schema.json")
        engine = IngestEngine(temp_data_lake, schema_path)
        
        # Test with fixture files
        fixture_dir = Path("tests/fixtures")
        
        for fixture_file in fixture_dir.glob("*.jsonl"):
            if fixture_file.name.endswith(".sample.jsonl"):
                stats = engine.ingest_file(fixture_file)
                
                # All fixture records should be valid
                assert stats["valid_records"] > 0
                assert stats["rejected_records"] == 0
                
                # Check that output files were created
                raw_files = list((temp_data_lake / "raw").glob(f"*{fixture_file.stem}*"))
                assert len(raw_files) > 0
    
    def test_full_pipeline(self, temp_data_lake):
        """Test the full ingest and normalize pipeline."""
        schema_path = Path("schemas/note_raw.schema.json")
        canonical_schema_path = Path("schemas/note_canonical.schema.json")
        
        # Step 1: Ingest
        ingest_engine = IngestEngine(temp_data_lake, schema_path)
        fixture_file = Path("tests/fixtures/A.sample.jsonl")
        
        ingest_stats = ingest_engine.ingest_file(fixture_file)
        assert ingest_stats["valid_records"] > 0
        
        # Step 2: Normalize
        normalize_engine = NormalizeEngine(temp_data_lake, canonical_schema_path)
        normalize_stats = normalize_engine.normalize_all_raw_files()
        
        assert len(normalize_stats) > 0
        total_normalized = sum(stats["normalized_records"] for stats in normalize_stats)
        assert total_normalized > 0
        
        # Check that canonical files were created
        canonical_files = list((temp_data_lake / "standardized").glob("*.jsonl"))
        assert len(canonical_files) > 0


if __name__ == "__main__":
    pytest.main([__file__])
