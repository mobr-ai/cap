"""
Vega util to convert data to vega format.
"""
import logging
from typing import Any
from opentelemetry import trace

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
    def _convert_to_vega_format(
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

            else:
                return {"values": []}

        except Exception as e:
            logger.error(f"Error converting to Vega format: {e}", exc_info=True)
            return {"values": []}

    @staticmethod
    def _convert_bar_chart(data: Any, user_query: str, sparql_query: str) -> dict[str, Any]:
        """Convert data to bar chart format."""
        if isinstance(data, list) and len(data) > 0:
            first_item = data[0]
            keys = list(first_item.keys())

            # Identify category (x-axis) and value (y-axis) fields
            category_candidates = VegaUtil.x_candidates
            category_key = next((k for k in keys if k.lower() in [c.lower() for c in category_candidates]), keys[0])

            # Value field is typically numeric - find first numeric field that's not the category
            value_key = None
            for k in keys:
                if k != category_key:
                    try:
                        # Check if values are numeric
                        val = first_item[k]
                        if isinstance(val, (int, float)) or (isinstance(val, str) and val.replace('.', '', 1).isdigit()):
                            value_key = k
                            break
                    except Exception as e:
                        logger.warning(f"Failed to convert value for {category_key}: {e}")
                        continue

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
                if isinstance(cat_val, dict):
                    cat_val = cat_val.get('value', str(cat_val))

                val = item.get(value_key, 0)
                if isinstance(val, dict):
                    val = val.get('ada', val.get('lovelace', val.get('value', 0)))

                try:
                    numeric_val = float(val)
                    # Convert ratios to percentages if needed
                    if 0 <= numeric_val <= 1:
                        numeric_val *= 100

                    values.append({
                        "category": str(cat_val),
                        "value": numeric_val
                    })
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping pie chart entry: {e}")
                    continue

            return {"values": values}

        return {"values": []}

    @staticmethod
    def _convert_line_chart(data: Any, user_query: str, sparql_query: str) -> dict[str, Any]:
        """Convert data to line chart format with multi-series support."""
        if not isinstance(data, list) or len(data) == 0:
            return {"values": []}

        first_item = data[0]
        keys = list(first_item.keys())

        # Identify x-axis field (typically time-based or sequential)
        x_candidates = VegaUtil.x_candidates
        x_key = next((k for k in keys if k.lower() in [c.lower() for c in x_candidates]), keys[0])

        # All other numeric fields are series
        series_keys = []
        for k in keys:
            if k != x_key:
                val = first_item[k]

                # Handle nested dicts
                if isinstance(val, dict):
                    val = val.get('ada', val.get('lovelace', val.get('value', None)))

                # Check if numeric
                try:
                    if val is not None:
                        if isinstance(val, (int, float)):
                            series_keys.append(k)
                        elif isinstance(val, str) and val.replace('.', '', 1).replace('-', '', 1).isdigit():
                            series_keys.append(k)
                except Exception as e:
                    logger.warning(f"Failed to check if {k} is numeric: {e}")
                    continue

        # Build line chart data with series index
        values = []
        for item in data:
            x_val = item.get(x_key)
            # Convert x to appropriate format
            if isinstance(x_val, dict):
                # Handle nested structures (like timestamps with 'value' key)
                x_val = x_val.get('value', str(x_val))

            # Extract date from datetime strings like "01T00:00:00.0"
            if isinstance(x_val, str):
                # Handle ISO-style datetime strings (e.g., "2021-03-01T00:00:00.0")
                if 'T' in x_val:
                    x_display = x_val.split('T')[0]  # Extract just the date part
                else:
                    x_display = x_val
            else:
                try:
                    x_display = float(x_val) if x_val is not None else 0
                except (ValueError, TypeError):
                    x_display = str(x_val) if x_val is not None else ""

            for series_idx, series_key in enumerate(series_keys):
                y_val = item.get(series_key)
                if y_val is not None:
                    try:
                        # Handle nested dict structures
                        if isinstance(y_val, dict):
                            y_val = y_val.get('value', y_val.get('ada', y_val.get('lovelace', 0)))

                        values.append({
                            "x": x_display,
                            "y": float(y_val),
                            "c": series_idx
                        })
                    except Exception as e:
                        logger.warning(f"Failed to build series {series_idx}: {e}")
                        continue

        return {"values": values}

    @staticmethod
    def _convert_table(data: Any, user_query: str, sparql_query: str) -> dict[str, Any]:
        """Convert data to table format."""
        if not isinstance(data, list) or len(data) == 0:
            return {"values": []}

        # Get all unique keys from all rows (in case structure varies)
        all_keys = []
        for item in data:
            for key in item.keys():
                if key not in all_keys:
                    all_keys.append(key)

        # Build column-based structure
        columns = []
        for idx, col_name in enumerate(all_keys):
            col_values = []
            for row in data:
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
                col_values.append(value)

            columns.append({
                f"col{idx + 1}": col_name,
                "values": col_values
            })

        return {"values": columns}