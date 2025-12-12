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

        # Restore temporal and ordering placeholders FIRST (they may contain other placeholders)
        restored = PlaceholderRestorer._restore_temporal_placeholders(restored, placeholder_map, current_values)
        restored = PlaceholderRestorer._restore_ordering_placeholders(restored, placeholder_map, current_values)

        # Sort placeholders by length (longest first) to avoid partial matches
        remaining_placeholders = [
            (ph, val) for ph, val in placeholder_map.items()
            if ph in restored and not ph.startswith(("<<YEAR_", "<<MONTH_", "<<PERIOD_", "<<ORDER_"))
        ]
        remaining_placeholders.sort(key=lambda x: len(x[0]), reverse=True)

        # Process remaining placeholders by type
        for placeholder, cached_value in remaining_placeholders:
            replacement = PlaceholderRestorer._get_replacement(
                placeholder, cached_value, placeholder_map, current_values
            )

            if replacement is not None:
                # Use word boundary to avoid partial matches
                restored = restored.replace(placeholder, replacement)

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
            return PlaceholderRestorer._restore_currency(placeholder, cached_value, current_values)
        elif placeholder.startswith("<<URI_"):
            return cached_value

        return None

    @staticmethod
    def _restore_currency(
        placeholder: str,
        cached_value: str,
        current_values: dict[str, list[str]]
    ) -> str:
        """Restore CURRENCY placeholder with cyclic fallback."""
        currencies = current_values.get("currencies", [])

        if currencies:
            try:
                idx = int(re.search(r'_(\d+)>>', placeholder).group(1))
                # Use modulo for cyclic access - always succeeds if list is non-empty
                currency_uri = currencies[idx % len(currencies)]
                currency_uri = currency_uri.strip('<>')
                return f"<{currency_uri}>"
            except (AttributeError, ValueError, IndexError) as e:
                logger.error(f"Error parsing CUR placeholder {placeholder}: {e}")

        # Fallback to cached value
        if cached_value:
            cached_value = cached_value.strip('<>')
            return f"<{cached_value}>"

        # Final fallback: use ADA as default currency
        logger.warning(f"No currency available for {placeholder}, using default ADA")
        return "<https://mobr.ai/ont/cardano#cnt/ada>"

    @staticmethod
    def _restore_inject(
        placeholder: str,
        inject_template: str,
        placeholder_map: dict[str, str],
        current_values: dict[str, list[str]]
    ) -> str:
        """Restore INJECT statement with nested placeholders."""

        replacement = inject_template
        # Sort nested placeholders by index to ensure correct order
        nested_placeholders = re.findall(
            r'<<(?:PCT_DECIMAL|PCT|NUM|STR|LIM|CUR|URI)_\d+>>',
            inject_template
        )

        # Sort by type and index to maintain extraction order
        def sort_key(ph):
            match = re.search(r'<<(\w+)_(\d+)>>', ph)
            return (match.group(1), int(match.group(2))) if match else ('', 0)

        nested_placeholders.sort(key=sort_key)

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
        """Get value from list using occurrence-based matching with cyclic fallback."""
        if not value_list:
            return cached_value or default

        try:
            match = re.search(r'_(\d+)>>', placeholder)
            if not match:
                return value_list[0]

            idx = int(match.group(1))
            # Always use modulo for safe cyclic access
            return value_list[idx % len(value_list)]

        except (ValueError, AttributeError):
            return cached_value or default

    @staticmethod
    def _restore_string(
        placeholder: str,
        current_values: dict[str, list[str]],
        cached_value: str
    ) -> str:
        """Restore string literal preserving quote style from cache."""
        # Preserve the quote style from cached value
        quote_char = '"'
        if cached_value:
            if cached_value.startswith("'"):
                quote_char = "'"
            elif cached_value.startswith('"'):
                quote_char = '"'

        tokens = current_values.get("tokens")
        if tokens:
            try:
                idx = int(re.search(r'_(\d+)>>', placeholder).group(1))
                if idx < len(tokens):
                    token = tokens[idx]
                    return f'{quote_char}{token}{quote_char}'
            except (ValueError, AttributeError, IndexError):
                pass

        return cached_value or '""'

    @staticmethod
    def _restore_temporal_placeholders(
        sparql: str,
        placeholder_map: dict[str, str],
        current_values: dict[str, list[str]]
    ) -> str:
        """Restore year and period placeholders."""

        # Restore period placeholders FIRST (they may contain year placeholders)
        for placeholder in sorted([k for k in placeholder_map.keys() if k.startswith("<<PERIOD_")]):
            if placeholder not in sparql:
                continue

            replacement = placeholder_map[placeholder]

            if current_values.get("temporal_periods"):
                period = current_values["temporal_periods"][0]

                # Only adjust if the cached pattern supports it
                cached_period_type = None
                if 'SUBSTR' in replacement:
                    if ', 1, 4)' in replacement:
                        cached_period_type = 'year'
                    elif ', 1, 7)' in replacement:
                        cached_period_type = 'month'
                    elif ', 9, 10)' in replacement:
                        cached_period_type = 'day'

                # Only modify if switching within compatible types
                if cached_period_type and period != cached_period_type:
                    period_map = {'year': (1, 4), 'month': (1, 7), 'day': (9, 10)}
                    if period in period_map and cached_period_type in period_map:
                        start, length = period_map[period]
                        replacement = re.sub(
                            r'SUBSTR\s*\([^,]+,\s*\d+\s*,\s*\d+\s*\)',
                            f'SUBSTR(STR(?timestamp), {start}, {length})',
                            replacement,
                            flags=re.IGNORECASE
                        )

            sparql = sparql.replace(placeholder, replacement)

        # Restore year placeholders AFTER periods
        for placeholder in sorted([k for k in placeholder_map.keys() if k.startswith("<<YEAR_")]):
            if placeholder not in sparql:
                continue

            if current_values.get("years"):
                idx = int(placeholder.replace('<<YEAR_', '').replace('>>', ''))
                cycle_idx = idx % len(current_values["years"])
                year = current_values["years"][cycle_idx]
                cached_value = placeholder_map[placeholder]
                replacement = re.sub(r'\d{4}', year, cached_value)
            else:
                replacement = placeholder_map[placeholder]

            sparql = sparql.replace(placeholder, replacement)

        # Restore month placeholders
        for placeholder in sorted([k for k in placeholder_map.keys() if k.startswith("<<MONTH_")]):
            if placeholder not in sparql:
                continue

            if current_values.get("months"):
                idx = int(placeholder.replace('<<MONTH_', '').replace('>>', ''))
                cycle_idx = idx % len(current_values["months"])
                month = current_values["months"][cycle_idx]
                cached_value = placeholder_map[placeholder]
                replacement = re.sub(
                    r'\d{4}-\d{2}|\b(january|february|march|april|may|june|july|august|september|october|november|december)\b',
                    month,
                    cached_value,
                    flags=re.IGNORECASE
                )
            else:
                replacement = placeholder_map[placeholder]

            sparql = sparql.replace(placeholder, replacement)

        # Restore duration literals (P7D, P30D, etc.)
        if current_values.get("durations"):
            duration = current_values["durations"][0]
            # Replace any duration pattern in the SPARQL
            sparql = re.sub(
                r'"P\d+[DWMY]"(?:\^\^xsd:(?:dayTimeDuration|duration))?',
                f'"{duration}"^^xsd:dayTimeDuration',
                sparql
            )

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
