"""
Identity resolution rules for building patient unit-of-record.
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Set

from ..matchers import IdentityMatcher
from ..writers import DataWriter


def build(standardized_dir: str, warehouse_root: str, artifacts_root: str) -> int:
    """
    Build patient unit-of-record from standardized notes.
    
    Args:
        standardized_dir: Path to standardized notes directory
        warehouse_root: Path to data warehouse root
        artifacts_root: Path to artifacts root
    
    Returns:
        Exit code (0 for success)
    """
    try:
        # Initialize components
        matcher = IdentityMatcher()
        writer = DataWriter(warehouse_root, artifacts_root)
        
        # Load standardized notes
        notes_data = {}
        note_links = []
        rule_counts = {"mrn_match": 0, "triplet_match": 0, "new_patient": 0}
        
        standardized_path = Path(standardized_dir)
        if not standardized_path.exists():
            print(f"Error: Standardized directory {standardized_dir} does not exist")
            return 1
        
        # Process all JSONL files in standardized directory
        for jsonl_file in standardized_path.glob("*.jsonl"):
            print(f"Processing {jsonl_file}")
            
            with open(jsonl_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        note = json.loads(line.strip())
                        note_uid = note.get("uid")
                        
                        if not note_uid:
                            print(f"Warning: Note without UID in {jsonl_file}:{line_num}")
                            continue
                        
                        # Store note data
                        notes_data[note_uid] = note
                        
                        # Match note to patient
                        match_result = matcher.match_note(note)
                        
                        # Count rule usage
                        rule = match_result.rule
                        if rule in rule_counts:
                            rule_counts[rule] += 1
                        
                        # Create note link
                        note_link = {
                            "note_uid": note_uid,
                            "patient_uid": match_result.patient_uid,
                            "rule": rule,
                            "mrn": note.get("mrn", ""),
                            "source_id": note.get("source_id", "")
                        }
                        note_links.append(note_link)
                        
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON in {jsonl_file}:{line_num}: {e}")
                        continue
        
        # Get results
        patient_groups = matcher.get_patient_groups()
        conflicts = matcher.get_conflicts()
        
        # Calculate statistics
        total_notes = len(notes_data)
        total_patients = len(patient_groups)
        
        stats = {
            "total_notes": total_notes,
            "total_patients": total_patients,
            "mrn_match_rate": rule_counts["mrn_match"] / total_notes if total_notes > 0 else 0,
            "triplet_match_rate": rule_counts["triplet_match"] / total_notes if total_notes > 0 else 0,
            "new_patient_rate": rule_counts["new_patient"] / total_notes if total_notes > 0 else 0,
            "rule_counts": rule_counts,
            "conflict_count": len(conflicts)
        }
        
        # Write outputs
        patients_file = writer.write_patient_records(patient_groups, notes_data)
        links_file = writer.write_note_links(note_links)
        conflicts_file = writer.write_conflicts(conflicts)
        report_file = writer.write_report(stats, conflicts)
        
        print(f"Successfully processed {total_notes} notes into {total_patients} patients")
        print(f"Output files:")
        print(f"  Patients: {patients_file}")
        print(f"  Note links: {links_file}")
        print(f"  Conflicts: {conflicts_file}")
        print(f"  Report: {report_file}")
        
        return 0
        
    except Exception as e:
        print(f"Error during build: {e}")
        return 1


def report(report_path: str) -> int:
    """
    Display identity resolution report.
    
    Args:
        report_path: Path to report.json file
    
    Returns:
        Exit code (0 for success)
    """
    try:
        report_file = Path(report_path)
        if not report_file.exists():
            print(f"Error: Report file {report_path} does not exist")
            return 1
        
        with open(report_file, 'r') as f:
            report_data = json.load(f)
        
        # Display summary
        summary = report_data.get("summary", {})
        print("Identity Resolution Report")
        print("=" * 50)
        print(f"Date: {report_data.get('date', 'Unknown')}")
        print(f"Timestamp: {report_data.get('timestamp', 'Unknown')}")
        print()
        print("Summary:")
        print(f"  Total Notes: {summary.get('total_notes', 0)}")
        print(f"  Total Patients: {summary.get('total_patients', 0)}")
        print(f"  Conflicts: {summary.get('conflict_count', 0)}")
        print()
        print("Match Rates:")
        print(f"  MRN Match: {summary.get('mrn_match_rate', 0):.1%}")
        print(f"  Triplet Match: {summary.get('triplet_match_rate', 0):.1%}")
        print(f"  New Patients: {summary.get('new_patient_rate', 0):.1%}")
        
        # Display conflicts if any
        conflicts = report_data.get("conflicts", [])
        if conflicts:
            print()
            print("Conflicts:")
            for i, conflict in enumerate(conflicts[:10], 1):  # Show first 10
                conflict_type = conflict.get("type", "unknown")
                if conflict_type == "mrn_conflict":
                    print(f"  {i}. MRN conflict: {conflict.get('mrn')} -> Patients {conflict.get('patient_1')}, {conflict.get('patient_2')}")
                elif conflict_type == "triplet_conflict":
                    print(f"  {i}. Triplet conflict: Patients {conflict.get('patient_1')}, {conflict.get('patient_2')}")
            
            if len(conflicts) > 10:
                print(f"  ... and {len(conflicts) - 10} more conflicts")
        
        return 0
        
    except Exception as e:
        print(f"Error reading report: {e}")
        return 1


def main():
    """CLI entry point."""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m resolve.rules build <standardized_dir> <warehouse_root> <artifacts_root>")
        print("  python -m resolve.rules report <report.json>")
        return 1
    
    command = sys.argv[1]
    
    if command == "build":
        if len(sys.argv) != 5:
            print("Usage: python -m resolve.rules build <standardized_dir> <warehouse_root> <artifacts_root>")
            return 1
        return build(sys.argv[2], sys.argv[3], sys.argv[4])
    
    elif command == "report":
        if len(sys.argv) != 3:
            print("Usage: python -m resolve.rules report <report.json>")
            return 1
        return report(sys.argv[2])
    
    else:
        print(f"Unknown command: {command}")
        print("Available commands: build, report")
        return 1


if __name__ == "__main__":
    sys.exit(main())

