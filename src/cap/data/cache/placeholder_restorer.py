"""
Redis client for caching SPARQL queries and natural language mappings.
"""
import logging
import re
from typing import Optional, Tuple
from opentelemetry import trace

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class PlaceholderRestorer:
    """Restore placeholders in SPARQL with actual values."""

    @staticmethod
    def restore(sparql: str, placeholder_map: dict[str, str], current_values: dict[str, list[str]]) -> str:
        """Restore placeholders with current values."""
        prefixes, query_body = PlaceholderRestorer._extract_prefixes(sparql)
        restored = query_body

        # Process placeholders by type
        for placeholder, cached_value in placeholder_map.items():
            replacement = PlaceholderRestorer._get_replacement(
                placeholder, cached_value, placeholder_map, current_values
            )

            if replacement is not None:
                pattern = re.escape(placeholder)
                restored = re.sub(pattern, replacement, restored)

        # Restore temporal and ordering placeholders
        restored = PlaceholderRestorer._restore_temporal_placeholders(restored, placeholder_map, current_values)
        restored = PlaceholderRestorer._restore_ordering_placeholders(restored, placeholder_map, current_values)

        if prefixes:
            restored = prefixes + "\n\n" + restored

        return restored

    @staticmethod
    def _extract_prefixes(sparql: str) -> Tuple[str, str]:
        """Extract PREFIX declarations."""
        prefix_pattern = r'^((?:PREFIX\s+\w+:\s*<[^>]+>\s*)+)'
        prefix_match = re.match(prefix_pattern, sparql, re.MULTILINE | re.IGNORECASE)

        if prefix_match:
            return prefix_match.group(1).strip(), sparql[prefix_match.end():].strip()
        return "", sparql

    @staticmethod
    def _get_replacement(
        placeholder: str,
        cached_value: str,
        placeholder_map: dict[str, str],
        current_values: dict[str, list[str]]
    ) -> Optional[str]:
        """Get replacement value for a placeholder."""
        if placeholder.startswith("<<INJECT_"):
            return PlaceholderRestorer._restore_inject(placeholder, cached_value, placeholder_map, current_values)
        elif placeholder.startswith("<<PCT_DECIMAL_"):
            return PlaceholderRestorer._get_cyclic_value(placeholder, current_values.get("percentages_decimal"), cached_value, "0.01")
        elif placeholder.startswith("<<PCT_"):
            return PlaceholderRestorer._get_cyclic_value(placeholder, current_values.get("percentages"), cached_value, "1")
        elif placeholder.startswith("<<NUM_"):
            return PlaceholderRestorer._get_cyclic_value(placeholder, current_values.get("numbers"), cached_value, "1")
        elif placeholder.startswith("<<STR_"):
            return PlaceholderRestorer._restore_string(placeholder, current_values, cached_value)
        elif placeholder.startswith("<<LIM_"):
            return PlaceholderRestorer._get_cyclic_value(placeholder, current_values.get("limits"), cached_value, "10")
        elif placeholder.startswith("<<CUR_"):
            return cached_value
        elif placeholder.startswith("<<URI_"):
            return cached_value

        return None

    @staticmethod
    def _restore_inject(
        placeholder: str,
        inject_template: str,
        placeholder_map: dict[str, str],
        current_values: dict[str, list[str]]
    ) -> str:
        """Restore INJECT statement with nested placeholders."""
        replacement = inject_template
        nested_placeholders = re.findall(r'<<(?:PCT_DECIMAL|PCT|NUM|STR|LIM|CUR|URI)_\d+>>', inject_template)

        for nested_ph in nested_placeholders:
            nested_replacement = PlaceholderRestorer._get_replacement(
                nested_ph,
                placeholder_map.get(nested_ph, ""),
                placeholder_map,
                current_values
            )
            if nested_replacement:
                replacement = replacement.replace(nested_ph, nested_replacement)

        return replacement

    @staticmethod
    def _get_cyclic_value(
        placeholder: str,
        value_list: Optional[list[str]],
        cached_value: str,
        default: str
    ) -> str:
        """Get value from list using cyclic indexing."""
        if value_list:
            try:
                idx = int(re.search(r'\d+', placeholder).group())
                cycle_idx = idx % len(value_list)
                return value_list[cycle_idx]
            except (ValueError, AttributeError):
                pass
        return cached_value or default

    @staticmethod
    def _restore_string(
        placeholder: str,
        current_values: dict[str, list[str]],
        cached_value: str
    ) -> str:
        """Restore string literal with proper quotes."""
        tokens = current_values.get("tokens")
        if tokens:
            try:
                idx = int(re.search(r'\d+', placeholder).group())
                cycle_idx = idx % len(tokens)
                token = tokens[cycle_idx]
                quote_char = cached_value[0] if cached_value and cached_value[0] in ['"', "'"] else '"'
                return f'{quote_char}{token}{quote_char}'
            except (ValueError, AttributeError):
                pass
        return cached_value or '""'

    @staticmethod
    def _restore_temporal_placeholders(
        sparql: str,
        placeholder_map: dict[str, str],
        current_values: dict[str, list[str]]
    ) -> str:
        """Restore year and period placeholders."""
        # Restore year placeholders
        for placeholder in [k for k in placeholder_map.keys() if k.startswith("<<YEAR_")]:
            if current_values.get("years"):
                idx = int(placeholder.replace('<<YEAR_', '').replace('>>', ''))
                cycle_idx = idx % len(current_values["years"])
                year = current_values["years"][cycle_idx]
                cached_value = placeholder_map[placeholder]
                replacement = re.sub(r'\d{4}', year, cached_value)
            else:
                replacement = placeholder_map[placeholder]

            pattern = re.escape(placeholder)
            sparql = re.sub(pattern, replacement, sparql)

        # Restore period placeholders
        for placeholder in [k for k in placeholder_map.keys() if k.startswith("<<PERIOD_")]:
            replacement = placeholder_map[placeholder]
            pattern = re.escape(placeholder)
            sparql = re.sub(pattern, replacement, sparql)

        return sparql

    @staticmethod
    def _restore_ordering_placeholders(
        sparql: str,
        placeholder_map: dict[str, str],
        current_values: dict[str, list[str]]
    ) -> str:
        """Restore ordering placeholders."""
        for placeholder in [k for k in placeholder_map.keys() if k.startswith("<<ORDER_")]:
            if current_values.get("orderings"):
                ordering = current_values["orderings"][0]
                direction = ordering.split(':')[1]
                cached_order = placeholder_map[placeholder]
                replacement = re.sub(r'\b(ASC|DESC)\b', direction, cached_order, flags=re.IGNORECASE)
            else:
                replacement = placeholder_map[placeholder]

            pattern = re.escape(placeholder)
            sparql = re.sub(pattern, replacement, sparql)

        return sparql
