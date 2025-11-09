"""
Redis client for caching SPARQL queries and natural language mappings.
"""
import logging
import re
from typing import Optional, Tuple
from opentelemetry import trace

from cap.data.cache.placeholder_counters import PlaceholderCounters

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class SPARQLNormalizer:
    """Handle SPARQL query normalization with placeholders."""

    def __init__(self):
        self.counters = PlaceholderCounters()
        self.placeholder_map: dict[str, str] = {}

    def normalize(self, sparql_query: str, counters: Optional[PlaceholderCounters] = None) -> Tuple[str, dict[str, str]]:
        """Extract literals and instances from SPARQL, replace with typed placeholders."""
        if counters:
            self.counters = counters

        self.placeholder_map = {}

        # Extract and preserve prefixes
        prefixes, query_body = self._extract_prefixes(sparql_query)

        # Process query body
        normalized = self._process_query_body(query_body)

        # Restore prefixes
        if prefixes:
            normalized = prefixes + "\n\n" + normalized

        return normalized, self.placeholder_map

    def normalize_with_shared_counters(
        self,
        sparql_query: str,
        shared_counters: PlaceholderCounters
    ) -> Tuple[str, dict[str, str]]:
        """Normalize using externally managed counters for sequential queries."""
        self.counters = shared_counters  # Use shared counters
        self.placeholder_map = {}

        prefixes, query_body = self._extract_prefixes(sparql_query)
        normalized = self._process_query_body(query_body)

        if prefixes:
            normalized = prefixes + "\n\n" + normalized

        return normalized, self.placeholder_map

    def _extract_prefixes(self, sparql_query: str) -> Tuple[str, str]:
        """Extract PREFIX declarations from SPARQL."""
        prefix_pattern = r'^((?:PREFIX\s+\w+:\s*<[^>]+>\s*)+)'
        prefix_match = re.match(prefix_pattern, sparql_query, re.MULTILINE | re.IGNORECASE)

        if prefix_match:
            prefixes = prefix_match.group(1).strip()
            query_body = sparql_query[prefix_match.end():].strip()
            return prefixes, query_body

        return "", sparql_query

    def _process_query_body(self, query_body: str) -> str:
        """Process query body and extract all patterns."""
        normalized = query_body

        # Order matters: INJECT first, then currency URIs, then other patterns
        normalized = self._extract_inject_statements(normalized)
        normalized = self._extract_currency_uris(normalized)
        normalized = self._extract_uris(normalized)
        normalized = self._extract_temporal_patterns(normalized)
        normalized = self._extract_order_clauses(normalized)
        normalized = self._extract_percentages(normalized)
        normalized = self._extract_string_literals(normalized)
        normalized = self._extract_limit_offset(normalized)
        normalized = self._extract_numbers(normalized)

        return normalized

    def _extract_inject_statements(self, text: str) -> str:
        """Extract INJECT statements with nested placeholders."""
        result = text
        pattern = r'INJECT(?:_FROM_PREVIOUS)?\('
        pos = 0

        while True:
            match = re.search(pattern, result[pos:], re.IGNORECASE)
            if not match:
                break

            start = pos + match.start()
            paren_count = 1
            i = start + len(match.group(0))

            while i < len(result) and paren_count > 0:
                if result[i] == '(':
                    paren_count += 1
                elif result[i] == ')':
                    paren_count -= 1
                i += 1

            if paren_count == 0:
                original = result[start:i]
                placeholder = f"<<INJECT_{self.counters.inject}>>"
                self.counters.inject += 1

                # Parameterize percentage decimals inside INJECT
                parameterized_inject = self._parameterize_inject_decimals(original)
                self.placeholder_map[placeholder] = parameterized_inject

                result = result[:start] + placeholder + result[i:]
                pos = start + len(placeholder)
            else:
                break

        return result

    def _parameterize_inject_decimals(self, inject_text: str) -> str:
        """Replace percentage decimals inside INJECT with placeholders."""
        result = inject_text
        pct_decimal_matches = list(re.finditer(r'\b(0\.\d+)\b', inject_text))

        for match in reversed(pct_decimal_matches):
            decimal_val = match.group(1)
            if 0 < float(decimal_val) < 1.0:
                pct_placeholder = f"<<PCT_DECIMAL_{self.counters.pct}>>"
                self.counters.pct += 1
                self.placeholder_map[pct_placeholder] = decimal_val
                result = result[:match.start()] + pct_placeholder + result[match.end():]

        return result

    def _extract_currency_uris(self, text: str) -> str:
        """Extract currency URIs."""
        # pattern captures the full URI including any digits
        pattern = r'<http://www\.mobr\.ai/ontologies/cardano#cnt/[^>]+>'
        matches = list(re.finditer(pattern, text))

        for match in reversed(matches):
            # Skip if already a placeholder
            if self._is_inside_placeholder(text, match):
                continue

            original = match.group(0)
            placeholder = f"<<CUR_{self.counters.cur}>>"
            self.counters.cur += 1
            self.placeholder_map[placeholder] = original
            text = text[:match.start()] + placeholder + text[match.end():]

        return text

    def _extract_temporal_patterns(self, text: str) -> str:
        """Extract temporal patterns (years, periods)."""

        # Extract period patterns FIRST (they may contain years)
        temporal_patterns = [
            (r'BIND\s*\(\s*SUBSTR\s*\(\s*STR\s*\(\s*\?timestamp\s*\)\s*,\s*1\s*,\s*7\s*\)\s+AS\s+\?timePeriod\s*\)', 'MONTH'),
            (r'BIND\s*\(\s*SUBSTR\s*\(\s*STR\s*\(\s*\?timestamp\s*\)\s*,\s*1\s*,\s*4\s*\)\s+AS\s+\?timePeriod\s*\)', 'YEAR'),
            (r'BIND\s*\(\s*SUBSTR\s*\(\s*STR\s*\(\s*\?timestamp\s*\)\s*,\s*9\s*,\s*10\s*\)\s+AS\s+\?timePeriod\s*\)', 'DAY'),
            (r'BIND\s*\(\s*CONCAT\s*\([^)]*SUBSTR[^)]*week[^)]*\)\s+AS\s+\?timePeriod\s*\)', 'WEEK'),
            (r'\?epoch\s+cardano:hasEpochNumber\s+\?timePeriod', 'EPOCH'),
            (r'GROUP\s+BY\s+\?timePeriod', 'GROUPED_PERIOD'),
        ]

        for pattern, period_type in temporal_patterns:
            matches = list(re.finditer(pattern, text, re.IGNORECASE))
            for match in reversed(matches):
                if self._is_inside_placeholder(text, match):
                    continue
                placeholder = f"<<PERIOD_{period_type}_{self.counters.period}>>"
                self.counters.period += 1
                self.placeholder_map[placeholder] = match.group(0)
                text = text[:match.start()] + placeholder + text[match.end():]

        # Extract year dateTime literals AFTER periods are extracted
        pattern = r'"(\d{4})-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z"\^\^xsd:dateTime'
        matches = list(re.finditer(pattern, text))
        for match in reversed(matches):
            if self._is_inside_placeholder(text, match):
                continue
            placeholder = f"<<YEAR_{self.counters.year}>>"
            self.counters.year += 1
            self.placeholder_map[placeholder] = match.group(0)
            text = text[:match.start()] + placeholder + text[match.end():]

        return text

    def _extract_order_clauses(self, text: str) -> str:
        """Extract ORDER BY clauses with DESC/ASC variants."""

        pattern = r'ORDER\s+BY\s+(?:DESC|ASC)?\s*\([^\)]+\)|ORDER\s+BY\s+\?[\w]+(?:\s+(?:ASC|DESC))?'

        for match in re.finditer(pattern, text, re.IGNORECASE):
            if self._is_inside_placeholder(text, match):
                continue

            original = match.group(0)
            placeholder = f"<<ORDER_{self.counters.order}>>"
            self.counters.order += 1
            self.placeholder_map[placeholder] = original
            text = text[:match.start()] + placeholder + text[match.end():]

        return text

    def _extract_percentages(self, text: str) -> str:
        """Extract percentage patterns."""
        pattern = r'(\d+(?:\.\d+)?)\s*%|0\.\d+'
        matches = list(re.finditer(pattern, text))

        for match in reversed(matches):
            if self._is_inside_placeholder(text, match):
                continue
            placeholder = f"<<PCT_{self.counters.pct}>>"
            self.counters.pct += 1
            self.placeholder_map[placeholder] = match.group(0)
            # Use slicing instead of replace to target exact position
            text = text[:match.start()] + placeholder + text[match.end():]

        return text

    def _extract_string_literals(self, text: str) -> str:
        """Extract string literals."""
        pattern = r'["\']([^"\']+)["\']'
        matches = list(re.finditer(pattern, text))

        for match in reversed(matches):
            if self._is_inside_placeholder(text, match):
                continue
            placeholder = f"<<STR_{self.counters.str}>>"
            self.counters.str += 1
            self.placeholder_map[placeholder] = match.group(0)
            # Use slicing instead of replace to target exact position
            text = text[:match.start()] + placeholder + text[match.end():]

        return text

    def _extract_limit_offset(self, text: str) -> str:
        """Extract LIMIT and OFFSET values."""
        pattern = r'(LIMIT|OFFSET)\s+(\d+)'
        for match in re.finditer(pattern, text, re.IGNORECASE):
            if self._is_inside_placeholder(text, match):
                continue
            placeholder = f"<<LIM_{self.counters.lim}>>"
            self.counters.lim += 1
            self.placeholder_map[placeholder] = match.group(2)
            text = re.sub(
                f'{match.group(1)}\\s+{re.escape(match.group(2))}',
                f'{match.group(1)} {placeholder}',
                text, count=1, flags=re.IGNORECASE
            )

        return text

    def _extract_uris(self, text: str) -> str:
        """Extract Cardano URIs."""
        pattern = r'(cardano:(?:addr|asset|stake|pool|tx)[a-zA-Z0-9]+)'
        matches = list(re.finditer(pattern, text))

        for match in reversed(matches):
            if self._is_inside_placeholder(text, match):
                continue
            placeholder = f"<<URI_{self.counters.uri}>>"
            self.counters.uri += 1
            self.placeholder_map[placeholder] = match.group(0)
            text = text[:match.start()] + placeholder + text[match.end():]

        return text

    def _extract_numbers(self, text: str) -> str:
        """Extract numeric values."""
        # Extract formatted numbers first
        text = self._extract_formatted_numbers(text)
        # Then extract plain numbers
        text = self._extract_plain_numbers(text)
        return text

    def _extract_formatted_numbers(self, text: str) -> str:
        """Extract formatted numbers (with separators)."""
        pattern = r'\b\d{1,3}(?:[,._]\d{3})+(?:\.\d+)?\b'
        matches = list(re.finditer(pattern, text))

        for match in reversed(matches):
            if self._should_skip_number(text, match):
                continue

            cleaned_num = re.sub(r'[,._]', '', match.group(0))
            placeholder = f"<<NUM_{self.counters.num}>>"
            self.counters.num += 1
            self.placeholder_map[placeholder] = cleaned_num
            text = text[:match.start()] + placeholder + text[match.end():]

        return text

    def _extract_plain_numbers(self, text: str) -> str:
        """Extract plain numbers."""
        pattern = r'\b\d{1,}\b'
        matches = list(re.finditer(pattern, text))

        for match in reversed(matches):
            if self._should_skip_number(text, match):
                continue

            placeholder = f"<<NUM_{self.counters.num}>>"
            self.counters.num += 1
            self.placeholder_map[placeholder] = match.group(0)
            text = text[:match.start()] + placeholder + text[match.end():]

        return text

    def _should_skip_number(self, text: str, match: re.Match) -> bool:
        """Determine if a number should be skipped during extraction."""

        # Skip if inside an existing placeholder
        if self._is_inside_placeholder(text, match):
            return True

        # Skip if inside a URI (angle brackets)
        # Check for <http://...> patterns that haven't been converted to placeholders yet
        before_start = max(0, match.start() - 100)
        before_context = text[before_start:match.start()]
        after_context = text[match.end():min(len(text), match.end() + 50)]

        # Check if we're inside angle brackets (URI)
        last_open = before_context.rfind('<')
        last_close = before_context.rfind('>')
        next_close = after_context.find('>')

        # If last < is after last >, and there's a > after us, we're inside angle brackets
        if last_open > last_close and next_close != -1:
            # Additionally check if it's an HTTP URI
            uri_context = text[before_start + last_open:match.end() + next_close + 1]
            if 'http://' in uri_context or 'https://' in uri_context:
                return True

        context_start = max(0, match.start() - 30)
        context_end = min(len(text), match.end() + 30)
        context = text[context_start:context_end]

        skip_patterns = [
            '://', '<http', 'www.', '.org', '.com',
            'XMLSchema', '/ontologies/', 'SUBSTR'
        ]

        if any(pattern in context for pattern in skip_patterns):
            return True

        # Check if it's a SUBSTR parameter
        substr_param_pattern = r'SUBSTR\s*\([^,]+,\s*' + re.escape(match.group(0)) + r'(?:\s*[,)])'
        if re.search(substr_param_pattern, text[max(0, match.start()-50):match.end()+10], re.IGNORECASE):
            return True

        return False

    def _is_inside_placeholder(self, text: str, match: re.Match) -> bool:
        """Check if match position is inside an existing placeholder."""
        # Look backwards from match position
        before_text = text[:match.start()]

        # Count unclosed placeholders before this position
        open_count = before_text.count('<<')
        close_count = before_text.count('>>')

        # If there are more opens than closes, we're inside a placeholder
        return open_count > close_count