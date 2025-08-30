"""
Schema validation utilities for ingest operations.
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
import jsonschema
from jsonschema import ValidationError

logger = logging.getLogger(__name__)


def load_schema(schema_path: Path) -> Dict[str, Any]:
    """Load JSON schema from file."""
    try:
        with open(schema_path, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        logger.error(f"Failed to load schema from {schema_path}: {e}")
        raise


def validate_against_schema(data: Dict[str, Any], schema: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Validate data against JSON schema.
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        jsonschema.validate(instance=data, schema=schema)
        return True, None
    except ValidationError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Validation error: {e}"


def validate_raw_note(note_data: Dict[str, Any], schema_path: Path) -> Tuple[bool, Optional[str]]:
    """Validate raw note data against note_raw schema."""
    schema = load_schema(schema_path)
    return validate_against_schema(note_data, schema)


def validate_canonical_note(note_data: Dict[str, Any], schema_path: Path) -> Tuple[bool, Optional[str]]:
    """Validate canonical note data against note_canonical schema."""
    schema = load_schema(schema_path)
    return validate_against_schema(note_data, schema)
