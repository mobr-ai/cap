"""
Result processor for SPARQL query results.
Handles hex decoding and data transformation.
"""
import re
import logging
from typing import Any

logger = logging.getLogger(__name__)


def decode_hex_value(value: str) -> str:
    """
    Decode hex string to UTF-8.

    Args:
        value: Hex string (e.g., "737061636563696e67")

    Returns:
        Decoded string or original if not valid hex
    """
    try:
        # Remove any whitespace
        value = value.strip()

        # Check if it looks like hex (even length, valid hex chars)
        if len(value) % 2 == 0 and re.match(r'^[0-9a-fA-F]+$', value):
            bytes_value = bytes.fromhex(value)
            decoded = bytes_value.decode('utf-8', errors='ignore')

            # Only return if result is printable ASCII/UTF-8
            if decoded.isprintable() or all(ord(c) < 128 for c in decoded):
                return decoded
    except Exception as e:
        logger.debug(f"Hex decode failed for '{value}': {e}")

    return value


def process_sparql_results(results: dict[str, Any]) -> dict[str, Any]:
    """
    Process SPARQL results, decoding hex values where appropriate.

    Args:
        results: Raw SPARQL results

    Returns:
        Processed results with decoded values
    """
    if not results or 'results' not in results:
        return results

    bindings = results.get('results', {}).get('bindings', [])

    for binding in bindings:
        for var, value_obj in binding.items():
            if not isinstance(value_obj, dict):
                continue

            value = value_obj.get('value', '')
            datatype = value_obj.get('datatype', '')

            # Check if it's a string type that might be hex
            if datatype == 'http://www.w3.org/2001/XMLSchema#string':
                decoded = decode_hex_value(value)
                if decoded != value:
                    value_obj['value'] = decoded
                    value_obj['decoded_from_hex'] = True

    return results


def format_results_for_display(results: dict[str, Any]) -> list[str]:
    """
    Format results into display-friendly list.

    Args:
        results: Processed SPARQL results

    Returns:
        List of formatted values
    """
    output = []

    if not results or 'results' not in results:
        return output

    bindings = results.get('results', {}).get('bindings', [])

    for binding in bindings:
        for var, value_obj in binding.items():
            if isinstance(value_obj, dict):
                value = value_obj.get('value', '')
                output.append(value)

    return output