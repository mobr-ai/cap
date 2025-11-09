"""
Redis client for caching SPARQL queries and natural language mappings.
"""
import logging
import re
from opentelemetry import trace

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class ValueExtractor:
    """Extract values from natural language queries."""

    TEMPORAL_PATTERNS = {
        r'\b(yearly|annually|per year|each year|every year)\b': 'year',
        r'\b(monthly|per month|each month|every month)\b': 'month',
        r'\b(weekly|per week|each week|every week)\b': 'week',
        r'\b(daily|per day|each day|every day)\b': 'day',
        r'\b(per epoch|each epoch|every epoch|by epoch)\b': 'epoch'
    }

    ORDERING_PATTERNS = {
        r'\b(first|earliest|oldest|initial)\b': 'ordering:ASC',
        r'\b(last|latest|newest|most recent|recent)\b': 'ordering:DESC',
        r'\b(largest|biggest|highest|maximum|max|greatest)\b': 'ordering:DESC',
        r'\b(smallest|lowest|minimum|min|least)\b': 'ordering:ASC',
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
        chart_pattern = r'\b(bar|line|pie|scatter)\s+(chart|graph)\b'
        for match in re.finditer(chart_pattern, nl_query, re.IGNORECASE):
            chart_type = match.group(1).lower()
            if chart_type not in values["chart_types"]:
                values["chart_types"].append(chart_type)

        # Extract currency/token URIs (add this new section)
        # Look for ADA references
        if re.search(r'\bADA\b', nl_query, re.IGNORECASE):
            values["currencies"].append("http://www.mobr.ai/ontologies/cardano#cnt/ada")

        # Extract temporal periods
        for pattern, period in ValueExtractor.TEMPORAL_PATTERNS.items():
            if re.search(pattern, nl_query, re.IGNORECASE) and period not in values["temporal_periods"]:
                values["temporal_periods"].append(period)

        # Extract years
        for match in re.finditer(r'\b(in|of|for|during)?\s*(\d{4})\b', nl_query):
            year = match.group(2)
            if 1900 <= int(year) <= 2100 and year not in values["years"]:
                values["years"].append(year)

        # Extract months
        values["months"] = []
        month_pattern = r'\b(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s*(\d{4})?\b'
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
        for pattern, ordering in ValueExtractor.ORDERING_PATTERNS.items():
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
        for match in re.finditer(r'top\s+(\d+)', nl_query, re.IGNORECASE):
            limit = match.group(1)
            if limit not in values["limits"]:
                values["limits"].append(limit)

    @staticmethod
    def _extract_tokens(nl_query: str, values: dict[str, list[str]]) -> None:
        """Extract token names."""
        token_pattern = r'\b([A-Z]{3,10})\b(?=\s+(?:holder|token|account|supply|balance))|(?:from\s+the\s+)([A-Z]{3,10})(?:\s+supply)'
        excluded_words = ['THE', 'FOR', 'TOP', 'MANY', 'MUCH', 'HOW', 'WHAT', 'WHICH', 'ARE', 'DEFINE', "SHOW", "LIST"]

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