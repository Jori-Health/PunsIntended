"""
Data writer for patient records, note links, and reports.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Any


class DataWriter:
    """
    Writes patient records, note links, and reports to the data warehouse.
    """
    
    def __init__(self, warehouse_root: str, artifacts_root: str):
        self.warehouse_root = Path(warehouse_root)
        self.artifacts_root = Path(artifacts_root)
        self.date_str = datetime.now().strftime("%Y%m%d")
    
    def write_patient_records(self, patient_groups: Dict[str, Set[str]], notes_data: Dict[str, Dict]) -> str:
        """
        Write patient unit-of-record files to the data warehouse.
        """
        patients_dir = self.warehouse_root / "patients" / self.date_str
        patients_dir.mkdir(parents=True, exist_ok=True)
        
        patients_file = patients_dir / "patients.jsonl"
        patient_records = []
        
        for patient_uid, note_uids in patient_groups.items():
            # Collect all notes for this patient
            patient_notes = [notes_data[uid] for uid in note_uids if uid in notes_data]
            
            if not patient_notes:
                continue
            
            # Extract MRNs from all notes
            mrn_set = set()
            for note in patient_notes:
                mrn = note.get("mrn", "")
                if mrn:
                    mrn_set.add(mrn)
            
            # Use demographics from first note (assuming consistency)
            first_note = patient_notes[0]
            demographics = first_note.get("demographics", {})
            
            # Extract diagnoses from all notes
            diagnoses = set()
            for note in patient_notes:
                content = note.get("content", {}).get("raw_text", "")
                if content:
                    # Simple diagnosis extraction - look for cancer terms
                    cancer_terms = [
                        "adenocarcinoma", "carcinoma", "sarcoma", "leukemia", "lymphoma",
                        "melanoma", "glioblastoma", "pancreatic cancer", "breast cancer",
                        "lung cancer", "colon cancer", "prostate cancer", "ovarian cancer"
                    ]
                    content_lower = content.lower()
                    for term in cancer_terms:
                        if term in content_lower:
                            diagnoses.add(term)
            
            # Collect sources
            sources = set()
            for note in patient_notes:
                source = note.get("source_id", "")
                if source:
                    sources.add(source)
            
            # Create patient record
            patient_record = {
                "patient_uid": patient_uid,
                "mrn_set": list(mrn_set),
                "demographics": demographics,
                "diagnoses": list(diagnoses),
                "provenance": {
                    "notes_linked": len(patient_notes),
                    "sources": list(sources)
                }
            }
            
            patient_records.append(patient_record)
        
        # Write to JSONL file
        with open(patients_file, 'w') as f:
            for record in patient_records:
                f.write(json.dumps(record) + '\n')
        
        return str(patients_file)
    
    def write_note_links(self, note_links: List[Dict]) -> str:
        """
        Write note-to-patient links to the data warehouse.
        """
        links_dir = self.warehouse_root / "links" / self.date_str
        links_dir.mkdir(parents=True, exist_ok=True)
        
        links_file = links_dir / "note_links.jsonl"
        
        with open(links_file, 'w') as f:
            for link in note_links:
                f.write(json.dumps(link) + '\n')
        
        return str(links_file)
    
    def write_conflicts(self, conflicts: List[Dict]) -> str:
        """
        Write conflicts to the data warehouse.
        """
        conflicts_dir = self.warehouse_root / "conflicts" / self.date_str
        conflicts_dir.mkdir(parents=True, exist_ok=True)
        
        conflicts_file = conflicts_dir / "conflicts.jsonl"
        
        with open(conflicts_file, 'w') as f:
            for conflict in conflicts:
                f.write(json.dumps(conflict) + '\n')
        
        return str(conflicts_file)
    
    def write_report(self, stats: Dict[str, Any], conflicts: List[Dict]) -> str:
        """
        Write identity resolution report to artifacts.
        """
        artifacts_dir = self.artifacts_root / "identity" / self.date_str
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        
        report_file = artifacts_dir / "report.json"
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "date": self.date_str,
            "statistics": stats,
            "conflicts": conflicts,
            "summary": {
                "total_patients": stats.get("total_patients", 0),
                "total_notes": stats.get("total_notes", 0),
                "conflict_count": len(conflicts),
                "mrn_match_rate": stats.get("mrn_match_rate", 0),
                "triplet_match_rate": stats.get("triplet_match_rate", 0),
                "new_patient_rate": stats.get("new_patient_rate", 0)
            }
        }
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        return str(report_file)
