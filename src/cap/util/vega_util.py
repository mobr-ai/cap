"""
Vega util to convert data to vega format.
"""
import logging
from typing import Any, Tuple, List, Optional
from opentelemetry import trace
from collections import Counter

from cap.util.cardano_scan import convert_entity_to_cardanoscan_link
from cap.util.epoch_util import epoch_to_date

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class VegaUtil:
    """Util to convert data to vega format."""

    x_candidates = [
        'yearMonth', 'year', 'month', 'date', 'timePeriod', 'timestamp', 'ts',
        'epoch', 'epochNumber', 'x', 'index', 'blockHeight', 'blockNumber',
        'name', 'label', 'category'
    ]

    @staticmethod
    def _is_numeric_value(value: Any) -> bool:
        """
        Determine if a value should be treated as numeric for visualization purposes.

        This is the central logic for distinguishing numeric values from categorical/temporal values.

        Args:
            value: The value to check (can be nested dict, str, int, float, etc.)

        Returns:
            True if the value should be treated as numeric, False if categorical/temporal
        """
        # Extract actual value from nested structures
        if isinstance(value, dict):
            value = value.get('value', value.get('ada', value.get('lovelace', None)))

        # Native numeric types are always numeric
        if isinstance(value, (int, float)):
            return True

        # Handle string values
        if isinstance(value, str):
            clean_val = value.strip()

            # Empty strings are not numeric
            if not clean_val:
                return False

            # Date-like strings (contains date separators) are categorical
            if '-' in clean_val or '/' in clean_val:
                return False

            # Time-like strings (contains time separators) are categorical
            if ':' in clean_val:
                return False

            # Very short strings (1-2 chars) are likely categorical codes
            # e.g., "00", "01", "Q1", "A", etc.
            if len(clean_val) <= 2:
                return False

            # Try to parse as float
            try:
                float_val = float(clean_val)
                # If successful and contains decimal point or scientific notation, it's numeric
                # Otherwise it might be a categorical ID (e.g., "12345" for an account number)
                return '.' in clean_val or 'e' in clean_val.lower() or 'E' in clean_val
            except ValueError:
                return False

        # Everything else is not numeric
        return False

    @staticmethod
    def _classify_fields(data: list[dict]) -> Tuple[List[str], List[str]]:
        """
        Classify all fields in the data as either categorical or numeric.

        Args:
            data: List of data items

        Returns:
            Tuple of (categorical_keys, numeric_keys)
        """
        if not data:
            return [], []

        first_item = data[0]
        categorical_keys = []
        numeric_keys = []

        for key in first_item.keys():
            value = first_item[key]

            if VegaUtil._is_numeric_value(value):
                numeric_keys.append(key)
            else:
                categorical_keys.append(key)

        return categorical_keys, numeric_keys

    @staticmethod
    def _get_x_candidates(first_item: dict, keys: list) -> list:
        x_candidates = VegaUtil.x_candidates.copy()

        # Extend x_candidates with any keys containing 'date' or having datetime values
        for k in keys:
            val = first_item[k]
            # Add keys with 'date' in the name
            if 'date' in k.lower() and k.lower() not in [c.lower() for c in x_candidates]:
                x_candidates.append(k)
            # Add keys with datetime type values
            elif isinstance(val, dict) and val.get('type') == 'datetime' and k.lower() not in [c.lower() for c in x_candidates]:
                x_candidates.append(k)

        return x_candidates

    @staticmethod
    def _format_column_name(column_name: str) -> str:
        """
        Convert camelCase/variable names to human-readable format.

        Examples:
            timePeriod -> Time Period
            blockProducedCount -> Block Count
            poolId -> Pool Id
        """
        # Split camelCase into words
        import re
        # Insert space before uppercase letters
        spaced = re.sub(r'([A-Z])', r' \1', column_name)
        # Split and capitalize each word
        words = spaced.split()

        if not words:
            return column_name

        # Capitalize all words
        formatted = ' '.join(word.capitalize() for word in words)
        return formatted.strip()

    @staticmethod
    def convert_to_vega_format(
        kv_results: dict[str, Any],
        user_query: str,
        sparql_query: str
    ) -> dict[str, Any]:
        """
        Convert kv_results to Vega-compatible format based on result_type and data structure.

        Args:
            kv_results: The key-value results from SPARQL
            user_query: Original natural language query for context
            sparql_query: SPARQL query for understanding data structure

        Returns:
            Dictionary with 'values' key containing formatted data for Vega
        """
        result_type = kv_results.get("result_type", "")
        data = kv_results.get("data", [])

        if not data:
            return {"values": []}

        try:
            if result_type == "bar_chart":
                return VegaUtil._convert_bar_chart(data, user_query, sparql_query)

            elif result_type == "pie_chart":
                return VegaUtil._convert_pie_chart(data, user_query, sparql_query)

            elif result_type == "line_chart":
                return VegaUtil._convert_line_chart(data, user_query, sparql_query)

            elif result_type == "table":
                return VegaUtil._convert_table(data, user_query, sparql_query)

            elif result_type == "scatter_chart":
                return VegaUtil._convert_scatter_chart(data, user_query, sparql_query)

            elif result_type == "bubble_chart":
                return VegaUtil._convert_bubble_chart(data, user_query, sparql_query)

            elif result_type == "treemap":
                return VegaUtil._convert_treemap(data, user_query, sparql_query)

            elif result_type == "heatmap":
                return VegaUtil._convert_heatmap(data, user_query, sparql_query)
            else:
                return {"values": []}

        except Exception as e:
            logger.error(f"Error converting to Vega format: {e}")
            return {"values": []}

    @staticmethod
    def _convert_bar_chart(data: Any, user_query: str, sparql_query: str) -> dict[str, Any]:
        """Convert data to bar chart format."""
        if isinstance(data, list) and len(data) > 0:
            first_item = data[0]
            keys = list(first_item.keys())

            # Identify category (x-axis) and value (y-axis) fields
            category_candidates = VegaUtil._get_x_candidates(first_item, keys)
            category_key = next((k for k in keys if k.lower() in [c.lower() for c in category_candidates]), keys[0])

            # Value field is typically numeric - find first numeric field that's not the category
            value_key = None
            for k in keys:
                if k != category_key and VegaUtil._is_numeric_value(first_item[k]):
                    value_key = k
                    break

            if not value_key:
                value_key = keys[-1] if len(keys) > 1 else keys[0]

            values = []
            for item in data:
                cat_val = item.get(category_key, "")
                if isinstance(cat_val, dict):
                    cat_val = cat_val.get('value', str(cat_val))

                amt_val = item.get(value_key, 0)
                if isinstance(amt_val, dict):
                    # Handle ADA/lovelace conversions
                    amt_val = amt_val.get('ada', amt_val.get('lovelace', amt_val.get('value', 0)))

                try:
                    values.append({
                        "category": str(cat_val),
                        "amount": float(amt_val)
                    })
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping bar chart entry: {e}")
                    continue

            return {"values": values}

        return {"values": []}

    @staticmethod
    def _convert_pie_chart(data: Any, user_query: str, sparql_query: str) -> dict[str, Any]:
        """Convert data to pie chart format."""
        # Pie chart data can be either a list or a nested dict
        if isinstance(data, dict):
            # Handle nested structure like the top holders example
            # Extract meaningful category-value pairs
            values = []

            # Try to find percentage/ratio fields
            for key, value in data.items():
                if isinstance(value, (int, float, str)):
                    try:
                        numeric_val = float(value)
                        # Convert ratios to percentages if needed
                        if 0 <= numeric_val <= 1:
                            numeric_val *= 100
                        values.append({
                            "category": key,
                            "value": numeric_val
                        })
                    except Exception as e:
                        logger.warning(f"Failed to convert value for key {key}: {e}")
                        continue

            # If we have meaningful data, return it; otherwise create a simple representation
            if values:
                return {"values": values}

        elif isinstance(data, list) and len(data) > 0:
            first_item = data[0]
            keys = list(first_item.keys())

            # Find category and value keys
            category_key = next((k for k in keys if k.lower() in ['category', 'label', 'name', 'group']), keys[0])
            value_key = next((k for k in keys if k != category_key), keys[-1])

            values = []
            for item in data:
                cat_val = item.get(category_key, "")
                val = item.get(value_key, 0)
                if isinstance(val, dict):
                    val = val.get('ada', val.get('lovelace', val.get('value', 0)))

                try:
                    values.append({
                        "category": str(cat_val),
                        "value": float(val)
                    })
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping pie chart entry: {e}")
                    continue

            return {"values": values}

        return {"values": []}

    @staticmethod
    def _detect_series_from_repetitions(data: list, x_key: str) -> bool:
        """
        Detect if data contains multiple series within a single y variable
        by checking for consistent x-value repetitions.

        Returns True if repetitions detected, False otherwise.
        """
        if len(data) < 2:
            return False

        # Count occurrences of each x value
        x_values = [item.get(x_key) for item in data]
        x_counts = Counter(x_values)

        # Check if there's a consistent repetition pattern (same count for all x values)
        unique_counts = set(x_counts.values())

        # If all x values repeat the same number of times (and > 1), we have multiple series
        if len(unique_counts) == 1 and list(unique_counts)[0] > 1:
            return True

        return False

    @staticmethod
    def _detect_repetition_pattern(data: list, x_key: str) -> int:
        """
        Detect if x values repeat consistently, indicating multiple series in one variable.

        Returns: Number of repetitions per x value (1 if no pattern detected)
        """
        if len(data) < 2:
            return 1

        x_values = [item.get(x_key) for item in data]
        x_counts = Counter(x_values)
        unique_counts = set(x_counts.values())

        # Consistent repetition pattern exists if all x values repeat the same number of times
        if len(unique_counts) == 1 and list(unique_counts)[0] > 1:
            return list(unique_counts)[0]

        return 1

    @staticmethod
    def _format_x_value(x_val: Any, x_key: str) -> str:
        """Extract and format x-axis value for line charts."""
        if isinstance(x_val, dict):
            x_val = x_val.get('value', str(x_val))

        if isinstance(x_val, str) and 'epoch' in x_key.lower():
            try:
                epoch_num = int(float(x_val))
                return epoch_to_date(epoch_num)
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to convert epoch {x_val}: {e}")
                return str(x_val)

        if not isinstance(x_val, str) and 'epoch' in x_key.lower():
            try:
                epoch_num = int(float(x_val)) if isinstance(x_val, str) else int(x_val)
                return epoch_to_date(epoch_num)
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to convert epoch {x_val}: {e}")

        return str(x_val) if x_val is not None else ""

    @staticmethod
    def _abbreviate_label(label: str, max_length: int = 11) -> str:
        """Abbreviate labels longer than max_length using ellipsis format."""
        if len(label) <= max_length:
            return label
        # Keep first 7 and last 4 characters for identifiable abbreviation
        prefix_len = min(7, max_length - 7)
        suffix_len = 4
        return f"{label[:prefix_len]}...{label[-suffix_len:]}"

    @staticmethod
    def _extract_series_labels(data: list, x_key: str, series_keys: list, repetition_count: int) -> list[str]:
        """
        Extract series labels from repeating patterns in non-series columns.

        Args:
            data: The data list
            x_key: The x-axis key
            series_keys: Keys used for series values
            repetition_count: Number of series detected

        Returns:
            List of labels for each series
        """
        if repetition_count <= 1:
            return []

        # Find columns that could contain series identifiers
        # (not x-axis, not y-axis/series values)
        first_item = data[0]
        candidate_keys = [k for k in first_item.keys()
                        if k != x_key and k not in series_keys]

        if not candidate_keys:
            return [f"Series {i+1}" for i in range(repetition_count)]

        # Use the first candidate column for labels
        label_key = candidate_keys[0]
        labels = []

        # Extract unique values in the order they appear (one per series)
        seen = set()
        for item in data:
            label_val = item.get(label_key, "")
            if isinstance(label_val, dict):
                label_val = label_val.get('value', str(label_val))
            label_str = str(label_val)

            if label_str not in seen:
                seen.add(label_str)
                labels.append(VegaUtil._abbreviate_label(label_str))

                if len(labels) == repetition_count:
                    break

        # Fill in any missing labels
        while len(labels) < repetition_count:
            labels.append(f"Series {len(labels)+1}")

        return labels

    @staticmethod
    def _extract_y_value(y_val: Any) -> Any:
        """Extract numeric value from potentially nested structures."""
        if isinstance(y_val, dict):
            extracted = y_val.get('value', y_val.get('ada', y_val.get('lovelace', None)))
            if extracted is None and isinstance(y_val, dict):
                return next(iter(y_val.values()), 0)
            return extracted
        return y_val

    @staticmethod
    def _convert_line_chart(data: Any, user_query: str, sparql_query: str) -> dict[str, Any]:
        """Convert data to line chart format with multi-series support."""
        if not isinstance(data, list) or len(data) == 0:
            logger.warning(f"cant serialize trend if it is not a list")
            return {"values": []}

        first_item = data[0]
        keys = list(first_item.keys())

        # Identify x-axis field (typically time-based or sequential)
        x_candidates = VegaUtil._get_x_candidates(first_item, keys)
        x_key = next((k for k in keys if k.lower() in [c.lower() for c in x_candidates]), keys[0])

        # All other numeric fields are series
        series_keys = []
        for k in keys:
            if k != x_key and VegaUtil._is_numeric_value(first_item[k]):
                series_keys.append(k)

        # If no series keys found, skip this conversion
        if not series_keys:
            logger.warning(f"No numeric series found in data for line chart")
            return {"values": []}

        # Build line chart data with series index
        values = []
        repetition_count = VegaUtil._detect_repetition_pattern(data, x_key)

        if repetition_count > 1 and len(series_keys) == 1:
            # Single variable contains multiple series via repetition
            for idx, item in enumerate(data):
                series_idx = idx % repetition_count
                x_display = VegaUtil._format_x_value(item.get(x_key), x_key)
                y_val = VegaUtil._extract_y_value(item.get(series_keys[0]))

                if y_val is not None:
                    try:
                        values.append({"x": x_display, "y": y_val, "c": series_idx})
                    except Exception as e:
                        logger.warning(f"Failed to build series {series_idx}: {e}")
        else:
            # Multiple variables each represent a series
            for item in data:
                x_display = VegaUtil._format_x_value(item.get(x_key), x_key)

                for series_idx, series_key in enumerate(series_keys):
                    y_val = VegaUtil._extract_y_value(item.get(series_key))

                    if y_val is not None:
                        try:
                            values.append({"x": x_display, "y": y_val, "c": series_idx})
                        except Exception as e:
                            logger.warning(f"Failed to build series {series_idx}: {e}")

        # Extract series labels if multiple series detected
        series_labels = []
        label_key = None
        if repetition_count > 1 and len(series_keys) == 1:
            # Find the column used for series identification
            first_item = data[0]
            candidate_keys = [k for k in first_item.keys()
                            if k != x_key and k not in series_keys]
            if candidate_keys:
                label_key = candidate_keys[0]

            series_labels = VegaUtil._extract_series_labels(
                data, x_key, series_keys, repetition_count
            )

        line_chart = {
            "values": values,
            "_series_labels": series_labels if series_labels else None,
            "_label_key": label_key,  # Which column was used for series labels
            "_x_key": x_key,  # X-axis column
            "_y_keys": series_keys  # Y-axis column(s)
        }
        logger.debug(f"converted to line chart: {line_chart}")
        return line_chart

    @staticmethod
    def _convert_scatter_chart(data: Any, user_query: str, sparql_query: str) -> dict[str, Any]:
        """Convert data to scatter chart format."""
        if not isinstance(data, list) or len(data) == 0:
            return {"values": []}

        first_item = data[0]
        keys = list(first_item.keys())

        # Find x and y fields (first two numeric fields)
        numeric_keys = [k for k in keys if VegaUtil._is_numeric_value(first_item[k])]

        if len(numeric_keys) < 2:
            logger.warning("Need at least 2 numeric fields for scatter chart")
            return {"values": []}

        x_key = numeric_keys[0]
        y_key = numeric_keys[1]

        # Optional: find category field for coloring
        category_key = None
        for k in keys:
            if k not in numeric_keys and isinstance(first_item[k], str):
                category_key = k
                break

        values = []
        for item in data:
            x_val = VegaUtil._extract_y_value(item.get(x_key))
            y_val = VegaUtil._extract_y_value(item.get(y_key))

            if x_val is not None and y_val is not None:
                try:
                    point = {"x": float(x_val), "y": float(y_val)}
                    if category_key:
                        cat_val = item.get(category_key, "")
                        if isinstance(cat_val, dict):
                            cat_val = cat_val.get('value', str(cat_val))
                        point["category"] = str(cat_val)
                    values.append(point)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping scatter point: {e}")
                    continue

        # Format column names for metadata
        formatted_columns = [
            VegaUtil._format_column_name(x_key),
            VegaUtil._format_column_name(y_key)
        ]
        if category_key:
            formatted_columns.append(VegaUtil._format_column_name(category_key))

        return {
            "values": values,
            "_x_key": x_key,
            "_y_key": y_key,
            "_category_key": category_key,
            "_columns": formatted_columns
        }

    @staticmethod
    def _convert_bubble_chart(data: Any, user_query: str, sparql_query: str) -> dict[str, Any]:
        """Convert data to bubble chart format (x, y, size)."""
        if not isinstance(data, list) or len(data) == 0:
            return {"values": []}

        first_item = data[0]
        keys = list(first_item.keys())

        # Find three numeric fields for x, y, and size
        numeric_keys = [k for k in keys if VegaUtil._is_numeric_value(first_item[k])]

        if len(numeric_keys) < 3:
            logger.warning("Need at least 3 numeric fields for bubble chart")
            return {"values": []}

        x_key = numeric_keys[0]
        y_key = numeric_keys[1]
        size_key = numeric_keys[2]

        # Optional: find category/label field
        label_key = None
        for k in keys:
            if k not in numeric_keys:
                label_key = k
                break

        values = []
        for item in data:
            x_val = VegaUtil._extract_y_value(item.get(x_key))
            y_val = VegaUtil._extract_y_value(item.get(y_key))
            size_val = VegaUtil._extract_y_value(item.get(size_key))

            if x_val is not None and y_val is not None and size_val is not None:
                try:
                    bubble = {
                        "x": float(x_val),
                        "y": float(y_val),
                        "size": float(size_val)
                    }
                    if label_key:
                        label_val = item.get(label_key, "")
                        if isinstance(label_val, dict):
                            label_val = label_val.get('value', str(label_val))
                        bubble["label"] = str(label_val)
                    values.append(bubble)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping bubble: {e}")
                    continue

        # Format column names for metadata
        formatted_columns = [
            VegaUtil._format_column_name(x_key),
            VegaUtil._format_column_name(y_key),
            VegaUtil._format_column_name(size_key)
        ]
        if label_key:
            formatted_columns.append(VegaUtil._format_column_name(label_key))

        return {
            "values": values,
            "_x_key": x_key,
            "_y_key": y_key,
            "_size_key": size_key,
            "_label_key": label_key,
            "_columns": formatted_columns
        }

    @staticmethod
    def _convert_treemap(data: Any, user_query: str, sparql_query: str) -> dict[str, Any]:
        """Convert data to treemap format (hierarchical structure)."""
        if not isinstance(data, list) or len(data) == 0:
            return {"values": []}

        first_item = data[0]
        keys = list(first_item.keys())

        # Find name/label and size fields
        name_key = next((k for k in keys if k.lower() in ['name', 'label', 'category', 'group', 'policyid', 'policy', 'token']), keys[0])

        # Find numeric field for size - use centralized numeric detection
        size_key = None
        for k in keys:
            if k != name_key and VegaUtil._is_numeric_value(first_item[k]):
                size_key = k
                break

        if not size_key:
            size_key = keys[-1] if len(keys) > 1 else keys[0]

        # Optional: find parent/group field for hierarchy
        parent_key = next((k for k in keys if k.lower() in ['parent', 'group', 'category'] and k != name_key), None)

        values = []
        for item in data:
            name_val = item.get(name_key, "")
            if isinstance(name_val, dict):
                name_val = name_val.get('value', str(name_val))

            size_val = VegaUtil._extract_y_value(item.get(size_key))

            if size_val is not None:
                try:
                    node = {
                        "name": str(name_val),
                        "value": float(size_val)
                    }
                    if parent_key:
                        parent_val = item.get(parent_key, "")
                        if isinstance(parent_val, dict):
                            parent_val = parent_val.get('value', str(parent_val))
                        node["parent"] = str(parent_val)
                    values.append(node)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping treemap node: {e}")
                    continue

        # Format column names for metadata
        formatted_columns = [
            VegaUtil._format_column_name(name_key),
            VegaUtil._format_column_name(size_key)
        ]
        if parent_key:
            formatted_columns.append(VegaUtil._format_column_name(parent_key))

        return {
            "values": values,
            "_name_key": name_key,
            "_size_key": size_key,
            "_parent_key": parent_key,
            "_columns": formatted_columns
        }

    @staticmethod
    def _convert_heatmap(data: Any, user_query: str, sparql_query: str) -> dict[str, Any]:
        """Convert data to heatmap format (x, y, value)."""
        if not isinstance(data, list) or len(data) == 0:
            return {"values": []}

        first_item = data[0]
        keys = list(first_item.keys())

        # Use centralized field classification
        categorical_keys, numeric_keys = VegaUtil._classify_fields(data)

        if len(categorical_keys) < 2:
            logger.warning(f"Need at least 2 categorical fields for heatmap, found {len(categorical_keys)}: {categorical_keys}")
            return {"values": []}

        x_key = categorical_keys[0]
        y_key = categorical_keys[1]
        value_key = numeric_keys[0] if numeric_keys else keys[-1]

        values = []
        for item in data:
            x_val = item.get(x_key, "")
            if isinstance(x_val, dict):
                x_val = x_val.get('value', str(x_val))

            y_val = item.get(y_key, "")
            if isinstance(y_val, dict):
                y_val = y_val.get('value', str(y_val))

            heat_val = VegaUtil._extract_y_value(item.get(value_key))

            if heat_val is not None:
                try:
                    values.append({
                        "x": str(x_val),
                        "y": str(y_val),
                        "value": float(heat_val)
                    })
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping heatmap cell: {e}")
                    continue

        # Format column names for metadata
        formatted_columns = [
            VegaUtil._format_column_name(x_key),
            VegaUtil._format_column_name(y_key),
            VegaUtil._format_column_name(value_key)
        ]

        return {
            "values": values,
            "_x_key": x_key,
            "_y_key": y_key,
            "_value_key": value_key,
            "_columns": formatted_columns
        }

    @staticmethod
    def _convert_url_to_link(value: str) -> str:
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

    @staticmethod
    def _convert_table(data: Any, user_query: str, sparql_query: str) -> dict[str, Any]:
        """Convert data to table format."""

        # Forcing list for one count results
        table_data = data
        if isinstance(data, dict):
            table_data = [data]

        if not isinstance(table_data, list) or len(table_data) == 0:
            logger.warning(f"Returning empty table for {user_query} with data {table_data}")
            return {"values": []}

        # Get all unique keys from all rows (in case structure varies)
        all_keys = []
        for item in table_data:
            for key in item.keys():
                if key not in all_keys:
                    all_keys.append(key)

        # Build column-based structure
        columns = []
        for idx, col_name in enumerate(all_keys):
            col_values = []
            for row in table_data:
                value = row.get(col_name, "")
                # Handle nested structures
                if isinstance(value, dict):
                    # Handle ADA conversions - prioritize ADA over lovelace
                    if 'ada' in value:
                        value = f"{value['ada']} ADA"
                    elif 'lovelace' in value:
                        value = value['lovelace']
                    elif 'decoded' in value and 'hex' in value:
                        # Token names - show decoded version
                        value = value['decoded']
                    elif 'value' in value:
                        value = value['value']
                    else:
                        # Fallback: try to get meaningful representation
                        value = str(value)

                elif isinstance(value, str):
                    if value.endswith(".0"):
                        value = value[:-2]

                # Convert URLs to clickable links
                # Convert blockchain entities to Cardanoscan links
                value = convert_entity_to_cardanoscan_link(col_name, value, sparql_query)

                # Convert ipfs (if not already converted)
                if not str(value).startswith('<a href='):
                    value = VegaUtil._convert_url_to_link(value)

                col_values.append(value)

            columns.append({
                f"col{idx + 1}": col_name,
                "values": col_values
            })

        return {"values": columns}