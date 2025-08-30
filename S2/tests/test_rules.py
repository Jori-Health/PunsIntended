import subprocess
import sys
import pathlib
import json
import tempfile
import shutil


def test_build_creates_outputs(repo_root, tmp_path):
    """Test that the build command creates all expected output files."""
    # Setup test data
    std_dir = tmp_path / "standardized"
    std_dir.mkdir()
    
    # Create sample standardized notes
    sample_notes = [
        {
            "uid": "note_1",
            "patient_id": "P001",
            "mrn": "MDA-1001",
            "source_id": "Source-A",
            "demographics": {"dob": "1980-01-01", "sex": "F"},
            "content": {"raw_text": "Pancreatic adenocarcinoma; FOLFIRINOX planned."},
            "provenance": {"source": "Source-A", "ingested_from": "/sources/A/P001.txt"}
        },
        {
            "uid": "note_2",
            "patient_id": "P001",
            "mrn": "1001",  # Same patient, different MRN format
            "source_id": "Source-B",
            "demographics": {"dob": "1980-01-01", "sex": "F"},
            "content": {"raw_text": "Pancreatic cancer follow-up."},
            "provenance": {"source": "Source-B", "ingested_from": "/sources/B/P001.txt"}
        },
        {
            "uid": "note_3",
            "patient_id": "P002",
            "mrn": "BMC-2001",
            "source_id": "Source-C",
            "demographics": {"dob": "1975-05-15", "sex": "M"},
            "content": {"raw_text": "Lung carcinoma diagnosis."},
            "provenance": {"source": "Source-C", "ingested_from": "/sources/C/P002.txt"}
        }
    ]
    
    # Write sample notes to JSONL file
    sample_file = std_dir / "sample_notes.jsonl"
    with open(sample_file, 'w') as f:
        for note in sample_notes:
            f.write(json.dumps(note) + '\n')
    
    # Setup output directories
    out_wh = tmp_path / "data_warehouse"
    out_art = tmp_path / "artifacts" / "identity"
    out_wh.mkdir(parents=True, exist_ok=True)
    out_art.mkdir(parents=True, exist_ok=True)
    
    # Run build command
    r = subprocess.run([
        sys.executable, "-m", "resolve.rules", "build", 
        str(std_dir), str(out_wh), str(out_art)
    ], capture_output=True, text=True, cwd=repo_root)
    
    assert r.returncode == 0, f"Build failed: {r.stderr}"
    
    # Check that output files were created
    date_str = "20241201"  # This will be the current date when run
    patients_file = out_wh / "patients" / date_str / "patients.jsonl"
    links_file = out_wh / "links" / date_str / "note_links.jsonl"
    conflicts_file = out_wh / "conflicts" / date_str / "conflicts.jsonl"
    report_file = out_art / "identity" / date_str / "report.json"
    
    # Note: The actual date will be dynamic, so we'll check for any date directory
    patients_dir = out_wh / "patients"
    links_dir = out_wh / "links"
    conflicts_dir = out_wh / "conflicts"
    report_dir = out_art / "identity"
    
    assert patients_dir.exists(), "Patients directory not created"
    assert links_dir.exists(), "Links directory not created"
    assert conflicts_dir.exists(), "Conflicts directory not created"
    assert report_dir.exists(), "Report directory not created"
    
    # Check that at least one date subdirectory exists in each
    patient_dates = list(patients_dir.iterdir())
    link_dates = list(links_dir.iterdir())
    conflict_dates = list(conflicts_dir.iterdir())
    report_dates = list(report_dir.iterdir())
    
    assert len(patient_dates) > 0, "No patient date directories created"
    assert len(link_dates) > 0, "No link date directories created"
    assert len(conflict_dates) > 0, "No conflict date directories created"
    assert len(report_dates) > 0, "No report date directories created"
    
    # Check that the expected files exist in the first date directory
    date_dir = patient_dates[0].name
    patients_file = out_wh / "patients" / date_dir / "patients.jsonl"
    links_file = out_wh / "links" / date_dir / "note_links.jsonl"
    conflicts_file = out_wh / "conflicts" / date_dir / "conflicts.jsonl"
    report_file = out_art / "identity" / date_dir / "report.json"
    
    assert patients_file.exists(), "patients.jsonl not created"
    assert links_file.exists(), "note_links.jsonl not created"
    assert conflicts_file.exists(), "conflicts.jsonl not created"
    assert report_file.exists(), "report.json not created"


def test_report_command(repo_root, tmp_path):
    """Test that the report command works correctly."""
    # Create a sample report
    report_dir = tmp_path / "artifacts" / "identity" / "20241201"
    report_dir.mkdir(parents=True, exist_ok=True)
    
    sample_report = {
        "timestamp": "2024-12-01T10:00:00",
        "date": "20241201",
        "statistics": {
            "total_notes": 3,
            "total_patients": 2,
            "mrn_match_rate": 0.67,
            "triplet_match_rate": 0.33,
            "new_patient_rate": 0.0
        },
        "conflicts": [],
        "summary": {
            "total_patients": 2,
            "total_notes": 3,
            "conflict_count": 0,
            "mrn_match_rate": 0.67,
            "triplet_match_rate": 0.33,
            "new_patient_rate": 0.0
        }
    }
    
    report_file = report_dir / "report.json"
    with open(report_file, 'w') as f:
        json.dump(sample_report, f)
    
    # Run report command
    r = subprocess.run([
        sys.executable, "-m", "resolve.rules", "report", str(report_file)
    ], capture_output=True, text=True, cwd=repo_root)
    
    assert r.returncode == 0, f"Report command failed: {r.stderr}"
    assert "Identity Resolution Report" in r.stdout
    assert "Total Notes: 3" in r.stdout
    assert "Total Patients: 2" in r.stdout

