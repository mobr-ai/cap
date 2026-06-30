import logging
import re
from datetime import datetime
from typing import Any, TypeAlias

from cap.chains.registry import get_chain

logger = logging.getLogger(__name__)

DataRow: TypeAlias = dict[str, Any]

class VegaValue:
    @classmethod
    def _is_numeric_value(cls, value: Any) -> bool:
        """
        Determine if a value should be treated as numeric for visualization purposes.
        """
        if isinstance(value, dict):
            datatype = str(value.get("datatype", "")).lower()
            literal_type = str(value.get("type", "")).lower()

            if literal_type == "literal" and any(t in datatype for t in (
                "int", "integer", "decimal", "float", "double",
                "long", "short", "byte", "nonnegativeinteger", "nonpositiveinteger",
                "positiveinteger", "negativeinteger"
            )):
                return True

        if isinstance(value, (int, float)):
            return True

        if isinstance(value, str):
            clean_val = value.strip()

            if not clean_val:
                return False

            if '-' in clean_val or '/' in clean_val:
                return False
            if ':' in clean_val:
                return False
            if len(clean_val) <= 2:
                return False

            try:
                float(clean_val)
                return True
            except ValueError:
                return False

        return False

    @classmethod
    def _extract_raw_value(cls, value: Any) -> Any:
        """Extract the underlying scalar value without applying visualization formatting."""
        if isinstance(value, dict):
            if "value" in value:
                return value["value"]
            return str(value)

        return value

    @classmethod
    def _parse_date_value(cls, value: Any) -> datetime | None:
        """Return a datetime only when the actual value has a recognized date format."""
        raw_value = cls._extract_raw_value(value)

        if isinstance(raw_value, datetime):
            return raw_value

        if not isinstance(raw_value, str):
            return None

        clean_value = raw_value.strip()
        if not clean_value:
            return None

        # Keep this strict to avoid treating ordinary labels, numeric values,
        # hashes, ratios, or ranges as dates.
        date_patterns = (
            # YYYY-MM
            r"^\d{4}-\d{2}$",

            # YYYY-MM-DD
            r"^\d{4}-\d{2}-\d{2}$",

            # ISO-8601 datetime
            r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}"
            r"(?::\d{2}(?:\.\d{1,6})?)?"
            r"(?:Z|[+-]\d{2}:?\d{2})?$",
        )

        if not any(re.match(pattern, clean_value) for pattern in date_patterns):
            return None

        if re.match(r"^\d{4}-\d{2}$", clean_value):
            clean_value = f"{clean_value}-01"

        try:
            # Support ISO-8601 UTC suffix
            normalized = clean_value.replace("Z", "+00:00")
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None

    @classmethod
    def _is_date_value(cls, value: Any) -> bool:
        """Determine date-ness from the value itself, not from the variable name."""
        return cls._parse_date_value(value) is not None

    @classmethod
    def _normalize_date_value(cls, value: Any) -> Any:
        dt = cls._parse_date_value(value)
        if dt is None:
            return value
        return dt.isoformat()

    @classmethod
    def _preprocess_values(cls, data: Any) -> Any:
        """Normalize values."""
        if isinstance(data, list):
            return [cls._preprocess_values(item) for item in data]

        if isinstance(data, dict):
            processed: dict[str, Any] = {}

            for key, value in data.items():
                if isinstance(value, str) and value.endswith(".0"):
                    value = value[:-2]
                    processed[key] = value
                    continue

                if isinstance(value, dict):
                    nested = dict(value)
                    if "value" in nested:
                        nested["value"] = cls._normalize_date_value(nested["value"])
                    processed[key] = nested
                else:
                    processed[key] = cls._normalize_date_value(value)

            return processed

        return cls._normalize_date_value(data)

    @classmethod
    def _format_display_value(cls, value: Any, key: str | None = None) -> str:
        """Format values used as visual labels across all chart types."""
        value = cls._extract_raw_value(value)

        if value is None:
            return ""

        return str(value)

    @classmethod
    def _format_x_value(cls, x_val: Any, x_key: str) -> str:
        """Extract and format x-axis value for charts."""
        x_val = cls._extract_raw_value(x_val)

        if isinstance(x_val, str) and "epoch" in x_key.lower():
            try:
                epoch_num = int(float(x_val))
                return get_chain().format_axis_value(x_key, epoch_num)
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to convert epoch {x_val}: {e}")
                return str(x_val)

        if "epoch" in x_key.lower():
            try:
                epoch_num = int(float(x_val))
                return get_chain().format_axis_value(x_key, epoch_num)
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to convert epoch {x_val}: {e}")
                return str(x_val)

        return cls._format_display_value(x_val)

    @classmethod
    def _abbreviate_label(cls, label: str, max_length: int = 11) -> str:
        """Abbreviate labels longer than max_length using ellipsis format."""
        if len(label) <= max_length:
            return label
        # Keep first 7 and last 4 characters for identifiable abbreviation
        prefix_len = min(7, max_length - 7)
        suffix_len = 4
        return f"{label[:prefix_len]}...{label[-suffix_len:]}"

    @classmethod
    def _extract_y_value(cls, y_val: Any) -> Any:
        """Extract the actual numeric value from potentially nested structures."""
        if isinstance(y_val, dict):
            if "value" in y_val:
                return y_val["value"]
            if "ada" in y_val:
                return y_val["ada"]
            return next(iter(y_val.values()), 0)
        return y_val

    @classmethod
    def _convert_url_to_link(cls, value: str) -> str:
        """Convert URL strings to HTML link format for Vega rendering."""
        value_str = str(value).strip()

        # Check if it's an IPFS URL
        if value_str.startswith('ipfs://'):
            ipfs_id = value_str[7:]  # Remove 'ipfs://' prefix
            href = f"https://ipfs.io/ipfs/{ipfs_id}"
            return f'<a href="{href}" target="_blank">{value_str}</a>'

        # Check if it's already an HTTPS URL
        elif value_str.startswith('https://') or value_str.startswith('http://'):
            return f'<a href="{value_str}" target="_blank">{value_str}</a>'

        # Not a URL, return as-is
        return value_str

    @classmethod
    def _format_column_name(cls, column_name: str) -> str:
        """Convert camelCase and snake_case variable names to human-readable format."""
        import re

        if not column_name:
            return column_name

        formatted = column_name.replace("_", " ")
        formatted = re.sub(r'(?<!^)([A-Z])', r' \1', formatted)
        words = formatted.split()

        return " ".join(word.capitalize() for word in words)
