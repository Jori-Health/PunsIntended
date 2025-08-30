"""
Identity resolution matcher for patient records.
"""

import hashlib
import re
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass


@dataclass
class MatchResult:
    """Result of a patient matching operation."""
    patient_uid: str
    rule: str
    confidence: float
    conflicts: List[str] = None


class IdentityMatcher:
    """
    Matches patient records across sources using MRN and demographic fallbacks.
    """
    
    def __init__(self):
        self.patient_groups: Dict[str, Set[str]] = {}  # patient_uid -> set of note_uids
        self.mrn_to_patient: Dict[str, str] = {}  # normalized_mrn -> patient_uid
        self.triplet_to_patient: Dict[str, str] = {}  # (dob, sex, dx_key) -> patient_uid
        self.conflicts: List[Dict] = []
    
    def normalize_mrn(self, mrn: str, source_id: str) -> str:
        """
        Normalize MRN per source to handle different formats.
        """
        if not mrn:
            return ""
        
        # Remove common prefixes/suffixes and standardize format
        mrn = str(mrn).strip().upper()
        
        # Source-specific normalization
        if source_id == "Source-A":
            # Remove "MDA-" prefix if present
            mrn = re.sub(r'^MDA-', '', mrn)
        elif source_id == "Source-B":
            # Remove "BMC-" prefix if present
            mrn = re.sub(r'^BMC-', '', mrn)
        elif source_id == "Source-C":
            # Remove "CANCER-" prefix if present
            mrn = re.sub(r'^CANCER-', '', mrn)
        
        return mrn
    
    def extract_diagnosis_key(self, content: str) -> str:
        """
        Extract a normalized diagnosis key from note content.
        """
        if not content:
            return ""
        
        # Convert to lowercase and extract common cancer terms
        content_lower = content.lower()
        
        # Look for common cancer diagnoses
        cancer_terms = [
            "adenocarcinoma", "carcinoma", "sarcoma", "leukemia", "lymphoma",
            "melanoma", "glioblastoma", "pancreatic", "breast", "lung", "colon",
            "prostate", "ovarian", "cervical", "uterine", "bladder", "kidney",
            "liver", "stomach", "esophageal", "thyroid", "brain", "bone"
        ]
        
        found_terms = []
        for term in cancer_terms:
            if term in content_lower:
                found_terms.append(term)
        
        # Sort for consistent ordering
        return "|".join(sorted(found_terms)) if found_terms else ""
    
    def create_triplet_key(self, note: Dict) -> str:
        """
        Create a key from DOB, sex, and diagnosis for fallback matching.
        """
        demographics = note.get("demographics", {})
        dob = demographics.get("dob", "")
        sex = demographics.get("sex", "")
        
        # Extract diagnosis from content
        content = note.get("content", {}).get("raw_text", "")
        dx_key = self.extract_diagnosis_key(content)
        
        return f"{dob}|{sex}|{dx_key}"
    
    def generate_patient_uid(self, keys: List[str]) -> str:
        """
        Generate a deterministic patient UID from matching keys.
        """
        # Sort keys for consistent ordering
        sorted_keys = sorted(keys)
        combined = "|".join(sorted_keys)
        
        # Create hash for deterministic UID
        hash_obj = hashlib.sha256(combined.encode())
        return f"P{hash_obj.hexdigest()[:8].upper()}"
    
    def match_note(self, note: Dict) -> MatchResult:
        """
        Match a note to an existing patient or create a new patient record.
        """
        note_uid = note.get("uid")
        source_id = note.get("source_id")
        mrn = note.get("mrn", "")
        
        # Normalize MRN
        normalized_mrn = self.normalize_mrn(mrn, source_id)
        
        # Strategy 1: MRN match
        if normalized_mrn and normalized_mrn in self.mrn_to_patient:
            patient_uid = self.mrn_to_patient[normalized_mrn]
            return MatchResult(
                patient_uid=patient_uid,
                rule="mrn_match",
                confidence=0.9
            )
        
        # Strategy 2: Triplet match (DOB + sex + diagnosis)
        triplet_key = self.create_triplet_key(note)
        if triplet_key and triplet_key in self.triplet_to_patient:
            patient_uid = self.triplet_to_patient[triplet_key]
            return MatchResult(
                patient_uid=patient_uid,
                rule="triplet_match",
                confidence=0.7
            )
        
        # Strategy 3: Create new patient
        # Generate UID from available identifiers
        uid_keys = []
        if normalized_mrn:
            uid_keys.append(f"mrn:{normalized_mrn}")
        if triplet_key:
            uid_keys.append(f"triplet:{triplet_key}")
        
        if not uid_keys:
            # Fallback to note UID if no other identifiers
            uid_keys.append(f"note:{note_uid}")
        
        new_patient_uid = self.generate_patient_uid(uid_keys)
        
        # Register the new patient
        self._register_patient(new_patient_uid, note, normalized_mrn, triplet_key)
        
        return MatchResult(
            patient_uid=new_patient_uid,
            rule="new_patient",
            confidence=1.0
        )
    
    def _register_patient(self, patient_uid: str, note: Dict, normalized_mrn: str, triplet_key: str):
        """
        Register a new patient and update all mappings.
        """
        note_uid = note.get("uid")
        
        # Initialize patient group if needed
        if patient_uid not in self.patient_groups:
            self.patient_groups[patient_uid] = set()
        
        # Add note to patient group
        self.patient_groups[patient_uid].add(note_uid)
        
        # Register MRN mapping
        if normalized_mrn:
            if normalized_mrn in self.mrn_to_patient:
                # Conflict: same MRN maps to different patients
                existing_patient = self.mrn_to_patient[normalized_mrn]
                if existing_patient != patient_uid:
                    self.conflicts.append({
                        "type": "mrn_conflict",
                        "mrn": normalized_mrn,
                        "patient_1": existing_patient,
                        "patient_2": patient_uid,
                        "note_uid": note_uid
                    })
            else:
                self.mrn_to_patient[normalized_mrn] = patient_uid
        
        # Register triplet mapping
        if triplet_key:
            if triplet_key in self.triplet_to_patient:
                # Conflict: same triplet maps to different patients
                existing_patient = self.triplet_to_patient[triplet_key]
                if existing_patient != patient_uid:
                    self.conflicts.append({
                        "type": "triplet_conflict",
                        "triplet": triplet_key,
                        "patient_1": existing_patient,
                        "patient_2": patient_uid,
                        "note_uid": note_uid
                    })
            else:
                self.triplet_to_patient[triplet_key] = patient_uid
    
    def get_conflicts(self) -> List[Dict]:
        """Get all detected conflicts."""
        return self.conflicts
    
    def get_patient_groups(self) -> Dict[str, Set[str]]:
        """Get all patient groups."""
        return self.patient_groups
