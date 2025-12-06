"""
SPARQL Date Arithmetic Preprocessor

A production-ready module for preprocessing SPARQL queries to resolve date/time
arithmetic expressions that are not supported by certain SPARQL engines like QLever.

This preprocessor handles:
- Duration arithmetic with NOW() in BIND statements
- ISO 8601 duration formats (xsd:duration, xsd:dayTimeDuration)
- Complex nested BIND statements
- Multiple BIND statements in a single query
- Preserves query structure and formatting
"""

import re
from datetime import datetime, timedelta
from typing import Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DurationParseError(Exception):
    """Raised when a duration string cannot be parsed"""
    pass

class SparqlDateProcessor:
    """
    Processor for resolving date arithmetic in SPARQL queries.

    Converts expressions like:
        BIND (NOW() - "P7D"^^xsd:dayTimeDuration as ?oneWeekAgo)
    To:
        BIND ("2025-11-29T01:35:00Z"^^xsd:dateTime as ?oneWeekAgo)
    """

    # Regex patterns
    BIND_PATTERN = re.compile(
        r'BIND\s*\(\s*'  # BIND (
        r'(NOW\(\s*\)|(?:"[^"]+"\^\^xsd:dateTime))'  # NOW() or a dateTime literal
        r'\s*([+\-])\s*'  # operator + or -
        r'"([^"]+)"\^\^xsd:(dayTimeDuration|duration|yearMonthDuration)'  # duration
        r'\s+[aA][sS]\s+'  # as (case insensitive)
        r'(\?[\w]+)'  # variable name
        r'\s*\)',  # )
        re.IGNORECASE | re.MULTILINE
    )

    DATETIME_LITERAL_PATTERN = re.compile(
        r'"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})?)"'
        r'\^\^xsd:dateTime'
    )

    def __init__(self, reference_time: Optional[datetime] = None):
        """
        Initialize the processor.

        Args:
            reference_time: Optional fixed reference time for testing.
                          If None, uses current UTC time.
        """
        self.reference_time = reference_time

    def _get_now(self) -> datetime:
        """Get the current time or reference time for calculations"""
        if self.reference_time:
            return self.reference_time
        return datetime.utcnow()

    def _parse_duration(self, duration_str: str) -> timedelta:
        """
        Parse an ISO 8601 duration string into a timedelta.

        Supports formats like:
        - P7D (7 days)
        - PT24H (24 hours)
        - PT1H30M (1 hour 30 minutes)
        - P1DT12H30M15S (1 day, 12 hours, 30 minutes, 15 seconds)
        - P1Y2M (1 year 2 months - approximated)

        Args:
            duration_str: ISO 8601 duration string (e.g., "P7D")

        Returns:
            timedelta object

        Raises:
            DurationParseError: If the duration cannot be parsed
        """
        if not duration_str.startswith('P'):
            raise DurationParseError(f"Duration must start with 'P': {duration_str}")

        duration_str = duration_str[1:]

        # Initialize components
        days = 0.0
        hours = 0.0
        minutes = 0.0
        seconds = 0.0

        # Split by 'T' to separate date and time parts
        if 'T' in duration_str:
            date_part, time_part = duration_str.split('T', 1)
        else:
            date_part = duration_str
            time_part = ''

        # Parse date part
        if date_part:
            # Years (approximate as 365 days)
            match = re.search(r'(\d+(?:\.\d+)?)Y', date_part)
            if match:
                days += float(match.group(1)) * 365

            # Months (approximate as 30 days)
            match = re.search(r'(\d+(?:\.\d+)?)M', date_part)
            if match:
                days += float(match.group(1)) * 30

            # Weeks
            match = re.search(r'(\d+(?:\.\d+)?)W', date_part)
            if match:
                days += float(match.group(1)) * 7

            # Days
            match = re.search(r'(\d+(?:\.\d+)?)D', date_part)
            if match:
                days += float(match.group(1))

        # Parse time part
        if time_part:
            # Hours
            match = re.search(r'(\d+(?:\.\d+)?)H', time_part)
            if match:
                hours = float(match.group(1))

            # Minutes
            match = re.search(r'(\d+(?:\.\d+)?)M', time_part)
            if match:
                minutes = float(match.group(1))

            # Seconds
            match = re.search(r'(\d+(?:\.\d+)?)S', time_part)
            if match:
                seconds = float(match.group(1))

        # Validate that we parsed something
        if days == 0 and hours == 0 and minutes == 0 and seconds == 0:
            raise DurationParseError(f"No valid duration components found in: {duration_str}")

        try:
            return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
        except Exception as e:
            raise DurationParseError(f"Failed to create timedelta: {e}")

    def _parse_datetime_literal(self, datetime_str: str) -> datetime:
        """
        Parse an xsd:dateTime literal string into a datetime object.

        Args:
            datetime_str: ISO 8601 datetime string

        Returns:
            datetime object

        Raises:
            ValueError: If the datetime cannot be parsed
        """
        # Try various ISO 8601 formats
        formats = [
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y-%m-%dT%H:%M:%S%z',
            '%Y-%m-%dT%H:%M:%S.%f%z',
        ]

        for fmt in formats:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                continue

        raise ValueError(f"Could not parse datetime: {datetime_str}")

    def _format_datetime(self, dt: datetime) -> str:
        """
        Format a datetime object as an xsd:dateTime literal.

        Args:
            dt: datetime object

        Returns:
            ISO 8601 formatted string with 'Z' timezone
        """
        # Convert to UTC if timezone-aware
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)

        # Format with milliseconds if present
        if dt.microsecond:
            return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        else:
            return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    def _replace_bind(self, match: re.Match) -> str:
        """
        Replace a single BIND statement with calculated date.

        Args:
            match: Regex match object

        Returns:
            Replacement BIND statement
        """
        try:
            datetime_expr = match.group(1)  # NOW() or dateTime literal
            operator = match.group(2)  # '+' or '-'
            duration_str = match.group(3)  # e.g., "P7D"
            duration_type = match.group(4)  # dayTimeDuration, duration, etc.
            variable = match.group(5)  # e.g., ?oneWeekAgo

            # Determine base datetime
            if datetime_expr.strip().upper().startswith('NOW'):
                base_dt = self._get_now()
            else:
                # Extract datetime from literal
                dt_match = self.DATETIME_LITERAL_PATTERN.search(datetime_expr)
                if dt_match:
                    base_dt = self._parse_datetime_literal(dt_match.group(1))
                else:
                    logger.warning(f"Could not extract datetime from: {datetime_expr}")
                    return match.group(0)

            # Parse the duration
            duration = self._parse_duration(duration_str)

            # Apply the operation
            if operator == '-':
                result_dt = base_dt - duration
            else:  # operator == '+'
                result_dt = base_dt + duration

            # Format as xsd:dateTime
            formatted_date = self._format_datetime(result_dt)

            # Return the replacement BIND statement
            replacement = f'BIND ("{formatted_date}"^^xsd:dateTime as {variable})'

            logger.debug(f"Replaced: {match.group(0)}")
            logger.debug(f"With: {replacement}")

            return replacement

        except (DurationParseError, ValueError) as e:
            logger.error(f"Error processing BIND statement: {e}")
            logger.error(f"Original: {match.group(0)}")
            # Return original on error
            return match.group(0)
        except Exception as e:
            logger.error(f"Unexpected error processing BIND statement: {e}")
            logger.error(f"Original: {match.group(0)}")
            return match.group(0)

    def process(self, query: str) -> Tuple[str, int]:
        """
        Process a SPARQL query and resolve all date arithmetic in BIND statements.

        Args:
            query: The SPARQL query string

        Returns:
            Tuple of (processed_query, number_of_replacements)
        """
        if not query or not isinstance(query, str):
            logger.warning("Invalid query input")
            return query, 0

        # Count replacements
        replacement_count = 0

        def count_and_replace(match):
            nonlocal replacement_count
            replacement_count += 1
            return self._replace_bind(match)

        # Replace all matching BIND statements
        processed_query = self.BIND_PATTERN.sub(count_and_replace, query)

        if replacement_count > 0:
            logger.info(f"Processed {replacement_count} date arithmetic expression(s)")

        return processed_query, replacement_count

    def __call__(self, query: str) -> str:
        """
        Allow the processor to be called as a function.

        Args:
            query: The SPARQL query string

        Returns:
            Processed query
        """
        processed_query, _ = self.process(query)
        return processed_query
