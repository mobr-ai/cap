"""
SPARQL Results to Key-Value Converter for Blockchain Data
Handles large integers (ADA amounts in lovelace) and nested structures
"""
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

def is_hex_string(value: str) -> bool:
    """
    Check if a string is a valid hexadecimal string.

    Args:
        value: String to check

    Returns:
        True if the string is valid hex, False otherwise
    """
    if not value or not isinstance(value, str):
        return False

    # Remove common hex prefixes
    clean_value = value.lower().strip()
    if clean_value.startswith('0x'):
        clean_value = clean_value[2:]

    # Check if it's all hex digits and has reasonable length
    if len(clean_value) < 2:
        return False

    return bool(re.match(r'^[0-9a-f]+$', clean_value))


def hex_to_string(hex_value: str) -> str:
    """
    Convert a hexadecimal string to a readable string.

    Args:
        hex_value: Hexadecimal string (with or without '0x' prefix)

    Returns:
        Decoded string, or original value if conversion fails
    """
    try:
        # Remove 0x prefix if present
        clean_hex = hex_value.lower().strip()
        if clean_hex.startswith('0x'):
            clean_hex = clean_hex[2:]

        # Convert hex to bytes
        byte_data = bytes.fromhex(clean_hex)

        # Try UTF-8 decoding first
        try:
            decoded = byte_data.decode('utf-8')
            # Only return if it contains printable characters
            if decoded.isprintable() or any(c.isalnum() or c.isspace() for c in decoded):
                return decoded.strip()
        except UnicodeDecodeError:
            pass

        # Try ASCII decoding as fallback
        try:
            decoded = byte_data.decode('ascii', errors='ignore')
            if decoded.strip():
                return decoded.strip()
        except:
            pass

        # If all decoding fails, return original
        return hex_value

    except (ValueError, TypeError) as e:
        logger.debug(f"Could not decode hex string '{hex_value}': {e}")
        return hex_value

def get_file_content(file_path: str) -> str:
    if file_path == "":
        return ""

    path = Path(file_path)
    if not path.exists():
        logger.warning(f"-- file not found: {file_path}")
        return ""

    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()

    return content
