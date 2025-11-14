"""
Redis client for caching SPARQL queries and natural language mappings.
"""
import logging
import re
from opentelemetry import trace

from cap.data.cache.pattern_registry import PatternRegistry

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
            PatternRegistry.build_pattern(PatternRegistry.FIRST_TERMS): 'ordering:ASC',
            PatternRegistry.build_pattern(PatternRegistry.LAST_TERMS): 'ordering:DESC',
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
            "chart_types": []
        }

        # Extract chart types
        chart_names = (
            PatternRegistry.BAR_CHART_TERMS +
            PatternRegistry.PIE_CHART_TERMS +
            PatternRegistry.LINE_CHART_TERMS +
            PatternRegistry.TABLE_TERMS
        )

        str_chart_names = '|'.join(re.escape(m) for m in chart_names)
        str_chart_sf_names = '|'.join(re.escape(m) for m in PatternRegistry.CHART_SUFFIXES)
        chart_pattern = rf'\b({str_chart_names})\s+({str_chart_sf_names})\b'
        for match in re.finditer(chart_pattern, nl_query, re.IGNORECASE):
            chart_type = match.group(1).lower()
            if chart_type not in values["chart_types"]:
                values["chart_types"].append(chart_type)

        # Extract currency/token URIs (add this new section)
        # Look for ADA references
        if re.search(r'\bADA\b', nl_query, re.IGNORECASE):
            if "http://www.mobr.ai/ontologies/cardano#cnt/ada" not in values["currencies"]:
                values["currencies"].append("http://www.mobr.ai/ontologies/cardano#cnt/ada")

        # Extract token names that might be currencies
        for token in values["tokens"]:
            # Construct potential currency URI
            currency_uri = f"http://www.mobr.ai/ontologies/cardano#cnt/{token.lower()}"
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
        ValueExtractor._extract_numbers(nl_query, values)

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
            limit = match.group(1)
            if limit not in values["limits"]:
                values["limits"].append(limit)

        # Explicit limits (latest N, first N, etc.)
        limit_terms = PatternRegistry.build_pattern(PatternRegistry.LAST_TERMS + PatternRegistry.FIRST_TERMS)
        for match in re.finditer(limit_terms + r'\s+(\d+)', nl_query, re.IGNORECASE):
            limit = match.group(2)
            if limit not in values["limits"]:
                values["limits"].append(limit)

        # Implicit limit of 1 for singular nouns without a number
        limit_pattern = PatternRegistry.build_pattern(PatternRegistry.LAST_TERMS + PatternRegistry.FIRST_TERMS)
        entity_pattern = PatternRegistry.build_pattern(PatternRegistry.DEFAULT_PRESERVED_EXPRESSIONS, word_boundary=False)
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
                num not in values["numbers"]):
                values["numbers"].append(num)
