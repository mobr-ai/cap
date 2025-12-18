"""
Redis client for caching SPARQL queries and natural language mappings.
"""
import logging
import re
from opentelemetry import trace

from cap.rdf.cache.pattern_registry import PatternRegistry

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class ValueExtractor:
    """Extract values from natural language queries."""

    @staticmethod
    def get_temporal_patterns() -> dict[str, str]:
        """Build temporal patterns from registry."""
        return {
            PatternRegistry.build_pattern(PatternRegistry.YEARLY_TERMS): 'year',
            PatternRegistry.build_pattern(PatternRegistry.MONTHLY_TERMS): 'month',
            PatternRegistry.build_pattern(PatternRegistry.WEEKLY_TERMS): 'week',
            PatternRegistry.build_pattern(PatternRegistry.DAILY_TERMS): 'day',
            PatternRegistry.build_pattern(PatternRegistry.EPOCH_PERIOD_TERMS): 'epoch'
        }

    @staticmethod
    def get_ordering_patterns() -> dict[str, str]:
        """Build ordering patterns from registry."""
        return {
            # Patterns with explicit numbers
            PatternRegistry.build_pattern(PatternRegistry.EARLIEST_TERMS) + r'\s+\d+': 'ordering:ASC',
            PatternRegistry.build_pattern(PatternRegistry.LATEST_TERMS) + r'\s+\d+': 'ordering:DESC',
            PatternRegistry.build_pattern(PatternRegistry.TOP_TERMS) + r'\s+\d+': 'ordering:DESC',
            PatternRegistry.build_pattern(PatternRegistry.BOTTOM_TERMS) + r'\s+\d+': 'ordering:ASC',
            # Patterns without numbers (implicit limit)
            PatternRegistry.build_pattern(PatternRegistry.EARLIEST_TERMS): 'ordering:ASC',
            PatternRegistry.build_pattern(PatternRegistry.LATEST_TERMS): 'ordering:DESC',
            PatternRegistry.build_pattern(PatternRegistry.TOP_TERMS): 'ordering:DESC',
            PatternRegistry.build_pattern(PatternRegistry.BOTTOM_TERMS): 'ordering:ASC',
            PatternRegistry.build_pattern(PatternRegistry.MAX_TERMS): 'ordering:DESC',
            PatternRegistry.build_pattern(PatternRegistry.MIN_TERMS): 'ordering:ASC',
        }

    @staticmethod
    def extract(nl_query: str) -> dict[str, list[str]]:
        """Extract all actual values from natural language query."""
        values = {
            "percentages": [],
            "percentages_decimal": [],
            "limits": [],
            "currencies": [],
            "tokens": [],
            "numbers": [],
            "temporal_periods": [],
            "years": [],
            "months": [],
            "orderings": [],
            "durations": [],
            "definitions": [],
            "quantifiers": [],
            "pool_ids": [],
        }

        # Extract currency/token URIs (add this new section)
        # Look for ADA references
        if re.search(r'\bADA\b', nl_query, re.IGNORECASE):
            if "https://mobr.ai/ont/cardano#cnt/ada" not in values["currencies"]:
                values["currencies"].append("https://mobr.ai/ont/cardano#cnt/ada")

        # Extract token names that might be currencies
        for token in values["tokens"]:
            # Construct potential currency URI
            currency_uri = f"https://mobr.ai/ont/cardano#cnt/{token.lower()}"
            if currency_uri not in values["currencies"]:
                values["currencies"].append(currency_uri)

        # Extract temporal periods
        for pattern, period in ValueExtractor.get_temporal_patterns().items():
            if re.search(pattern, nl_query, re.IGNORECASE) and period not in values["temporal_periods"]:
                values["temporal_periods"].append(period)

        # Extract years
        str_time_prep_names = '|'.join(re.escape(m) for m in PatternRegistry.TEMPORAL_PREPOSITIONS)
        for match in re.finditer(rf'\b({str_time_prep_names})?\s*(\d{4})\b', nl_query):
            year = match.group(2)
            if 1900 <= int(year) <= 2100 and year not in values["years"]:
                values["years"].append(year)

        for match in re.finditer(rf'(?:{str_time_prep_names}\s+)?(\d{{4}})\b', nl_query):
            year = match.group(1)
            if 1900 <= int(year) <= 2100 and year not in values["years"]:
                values["years"].append(year)


        # Extract months
        values["months"] = []
        month_names = PatternRegistry.MONTH_NAMES + PatternRegistry.MONTH_ABBREV
        str_month_names = '|'.join(re.escape(m) for m in month_names)
        month_pattern = rf'\b({str_month_names})\s*(\d{4})?\b'
        for match in re.finditer(month_pattern, nl_query, re.IGNORECASE):
            month = match.group(1).lower()
            year = match.group(2)
            if year:
                month_str = f"{month}-{year}"
            else:
                month_str = month
            if month_str not in values["months"]:
                values["months"].append(month_str)

        # Extract ordering
        for pattern, ordering in ValueExtractor.get_ordering_patterns().items():
            if re.search(pattern, nl_query, re.IGNORECASE) and ordering not in values["orderings"]:
                values["orderings"].append(ordering)

        # Extract percentages
        ValueExtractor._extract_percentages(nl_query, values)
        ValueExtractor._extract_limits(nl_query, values)
        ValueExtractor._extract_tokens(nl_query, values)
        ValueExtractor._extract_pool_ids(nl_query, values)
        ValueExtractor._extract_numbers(nl_query, values)
        ValueExtractor._extract_durations(nl_query, values)

        # Extract definition terms
        for pattern in PatternRegistry.DEFINITION_TERMS:
            if re.search(r'\b' + re.escape(pattern) + r'\b', nl_query, re.IGNORECASE):
                if pattern not in values["definitions"]:
                    values["definitions"].append(pattern)
                    break  # Only need one

        # Extract quantification terms
        for pattern in PatternRegistry.COUNT_TERMS:
            if re.search(r'\b' + re.escape(pattern) + r'\b', nl_query, re.IGNORECASE):
                if pattern not in values["quantifiers"]:
                    values["quantifiers"].append(pattern)
                    break  # Only need one

        logger.info(f"Extracted values from '{nl_query}': {values}")
        return values

    @staticmethod
    def _extract_percentages(nl_query: str, values: dict[str, list[str]]) -> None:
        """Extract percentage values."""
        # Extract percentages with % symbol
        for match in re.finditer(r'(\d+(?:\.\d+)?)\s*%', nl_query, re.IGNORECASE):
            pct = match.group(1)
            if pct not in values["percentages"]:
                values["percentages"].append(pct)
                decimal = float(pct) / 100
                values["percentages_decimal"].append(str(decimal))

        # Extract "N percent" format
        for match in re.finditer(r'(\d+(?:\.\d+)?)\s+percent', nl_query, re.IGNORECASE):
            pct = match.group(1)
            if pct not in values["percentages"]:
                values["percentages"].append(pct)
                decimal = float(pct) / 100
                values["percentages_decimal"].append(f"{decimal:.2f}")

        # Extract decimal percentages
        for match in re.finditer(r'\b(0\.\d+)\b', nl_query):
            decimal = match.group(1)
            decimal_float = float(decimal)
            if 0 < decimal_float < 1.0 and decimal not in values["percentages_decimal"]:
                values["percentages_decimal"].append(decimal)
                pct = str(decimal_float * 100).rstrip('0').rstrip('.')
                if pct not in values["percentages"]:
                    values["percentages"].append(pct)

    @staticmethod
    def _extract_limits(nl_query: str, values: dict[str, list[str]]) -> None:
        """Extract limit values."""
        # Explicit limits (top N)
        str_top_names = '|'.join(re.escape(m) for m in PatternRegistry.TOP_TERMS)
        for match in re.finditer(rf'\b({str_top_names})\s+(\d+)\b', nl_query, re.IGNORECASE):
            limit = match.group(2)
            if limit not in values["limits"]:
                values["limits"].append(limit)

        # Explicit limits (latest N, first N, etc.)
        limit_terms = PatternRegistry.build_pattern(PatternRegistry.LATEST_TERMS + PatternRegistry.EARLIEST_TERMS)
        for match in re.finditer(limit_terms + r'\s+(\d+)(?!\s*(?:hour|day|week|month|year|epoch)s?)\b', nl_query, re.IGNORECASE):
            limit = match.group(2)
            if limit not in values["limits"]:
                values["limits"].append(limit)

        # Implicit limit of 1 for singular nouns without a number
        limit_pattern = PatternRegistry.build_pattern(PatternRegistry.LATEST_TERMS + PatternRegistry.EARLIEST_TERMS)
        entity_pattern = PatternRegistry.build_pattern(PatternRegistry.get_entities(), word_boundary=False)
        if re.search(limit_pattern + r'\s+' + entity_pattern + r'\b(?!s)', nl_query, re.IGNORECASE):
            if "1" not in values["limits"]:
                values["limits"].append("1")

    @staticmethod
    def _extract_tokens(nl_query: str, values: dict[str, list[str]]) -> None:
        """Extract token names."""
        token_pattern = r'\b([A-Z]{3,10})\b(?=\s+(?:holder|token|account|supply|balance))|(?:from\s+the\s+)([A-Z]{3,10})(?:\s+supply)'
        excluded_words = PatternRegistry.FILLER_WORDS

        for match in re.finditer(token_pattern, nl_query):
            token = (match.group(1) or match.group(2)).upper()
            if token not in values["tokens"] and token not in excluded_words:
                values["tokens"].append(token)

    @staticmethod
    def _extract_pool_ids(nl_query: str, values: dict[str, list[str]]) -> None:
        """Extract Cardano pool IDs."""
        pool_pattern = r'\b(pool1[a-z0-9]{53})\b'
        for match in re.finditer(pool_pattern, nl_query, re.IGNORECASE):
            pool_id = match.group(1).lower()
            if pool_id not in values["pool_ids"]:
                values["pool_ids"].append(pool_id)

    @staticmethod
    def _extract_durations(nl_query: str, values: dict[str, list[str]]) -> None:
        """Extract duration expressions and convert to XSD duration format."""

        # Map units to XSD duration codes
        unit_to_code = {
            'day': ('D', 1), 'days': ('D', 1),
            'week': ('W', 7), 'weeks': ('W', 7),  # Store as days for consistency
            'month': ('M', 30), 'months': ('M', 30),
            'year': ('Y', 365), 'years': ('Y', 365)
        }

        # Pattern: "last N days/weeks/months/years"
        for match in re.finditer(
            r'\b(last|past|previous)\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)\b',
            nl_query,
            re.IGNORECASE
        ):
            num = int(match.group(2))
            unit = match.group(3).lower()

            if unit in unit_to_code:
                code, multiplier = unit_to_code[unit]
                # Convert everything to days for consistent comparison
                total_days = num * multiplier
                duration = f"P{total_days}D"
                if duration not in values["durations"]:
                    values["durations"].append(duration)

        # Pattern: "last week/month/year" (implicit 1)
        for match in re.finditer(
            r'\b(last|past|previous)\s+(day|week|month|year)\b',
            nl_query,
            re.IGNORECASE
        ):
            unit = match.group(2).lower()

            if unit in unit_to_code:
                code, multiplier = unit_to_code[unit]
                duration = f"P{multiplier}D"
                if duration not in values["durations"]:
                    values["durations"].append(duration)

    @staticmethod
    def _extract_numbers(nl_query: str, values: dict[str, list[str]]) -> None:
        """Extract numeric values."""
        # Extract text-formatted numbers (billion, million, etc.)
        multipliers = {'hundred': 100, 'thousand': 1000, 'million': 1000000, 'billion': 1000000000}

        for match in re.finditer(
            r'\b(\d+(?:\.\d+)?)\s+(billion(?:s)?|million(?:s)?|thousand(?:s)?|hundred(?:s)?)\b',
            nl_query, re.IGNORECASE
        ):
            num = match.group(1)
            unit = match.group(2).lower().rstrip('s')
            base_num = float(num)
            actual_value = str(int(base_num * multipliers.get(unit, 1)))

            context = nl_query[max(0, match.start()-20):min(len(nl_query), match.end()+10)]
            if 'ADA' in context.upper():
                lovelace_value = str(int(actual_value) * 1000000)
                if lovelace_value not in values["numbers"]:
                    values["numbers"].append(lovelace_value)
            else:
                if actual_value not in values["numbers"]:
                    values["numbers"].append(actual_value)

        # Extract formatted numbers
        for match in re.finditer(r'\b\d{1,3}(?:[,._]\d{3})+(?:\.\d+)?\b', nl_query):
            num = match.group(0)
            normalized_num = re.sub(r'[,._]', '', num)

            if (normalized_num not in values["limits"] and
                normalized_num not in values["percentages"] and
                normalized_num not in values["percentages_decimal"] and
                normalized_num not in values["years"] and
                normalized_num not in values["numbers"]):

                context = nl_query[max(0, match.start()-20):min(len(nl_query), match.end()+10)]
                if 'ADA' in context.upper():
                    lovelace_value = str(int(normalized_num) * 1000000)
                    values["numbers"].append(lovelace_value)
                else:
                    values["numbers"].append(normalized_num)

        # Extract simple numbers
        for match in re.finditer(r'\b\d+(?:\.\d+)?\b', nl_query):
            num = match.group(0)
            if re.search(r'\b\d{1,3}[,._]\d', nl_query[max(0, match.start()-1):match.end()+2]):
                continue

            if (num not in values["limits"] and
                    num not in values["percentages"] and
                    num not in values["percentages_decimal"] and
                    num not in values["years"] and
                    num not in values["numbers"]):
                values["numbers"].append(num)
