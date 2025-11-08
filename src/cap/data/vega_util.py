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
            # Common category fields: month, year, yearMonth, epoch, name, label
            category_candidates = ['month', 'yearMonth', 'timePeriod', 'year', 'epoch', 'epochNumber', 'name', 'label', 'category']
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
                    except:
                        continue

            if not value_key:
                value_key = keys[-1] if len(keys) > 1 else keys[0]

            return {
                "values": [
                    {
                        "category": str(item.get(category_key, "")),
                        "amount": float(item.get(value_key, 0))
                    }
                    for item in data
                ]
            }

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
                    except:
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

            return {
                "values": [
                    {
                        "category": str(item.get(category_key, "")),
                        "value": float(item.get(value_key, 0))
                    }
                    for item in data
                ]
            }

        return {"values": []}

    @staticmethod
    def _convert_line_chart(data: Any, user_query: str, sparql_query: str) -> dict[str, Any]:
        """Convert data to line chart format with multi-series support."""
        if not isinstance(data, list) or len(data) == 0:
            return {"values": []}

        first_item = data[0]
        keys = list(first_item.keys())

        # Identify x-axis field (typically time-based or sequential)
        x_candidates = ['yearMonth', 'year', 'month', 'date', 'timePeriod', 'timestamp', 'epoch', 'epochNumber', 'block', 'x', 'index']
        x_key = next((k for k in keys if k.lower() in [c.lower() for c in x_candidates]), keys[0])

        # All other numeric fields are series
        series_keys = []
        for k in keys:
            if k != x_key:
                try:
                    val = first_item[k]
                    # Check if it's numeric or can be converted to numeric
                    if isinstance(val, (int, float)) or (isinstance(val, str) and val.replace('.', '', 1).replace('-', '', 1).isdigit()):
                        series_keys.append(k)
                except:
                    continue

        # Build line chart data with series index
        values = []
        for item in data:
            x_val = item.get(x_key)
            # Convert x to appropriate format
            if isinstance(x_val, str):
                # Try to parse as date or keep as string
                x_display = x_val
            else:
                x_display = float(x_val) if x_val is not None else 0

            for series_idx, series_key in enumerate(series_keys):
                y_val = item.get(series_key)
                if y_val is not None:
                    try:
                        values.append({
                            "x": x_display,
                            "y": float(y_val),
                            "c": series_idx
                        })
                    except:
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
                # Handle nested structures like timestamps
                if isinstance(value, dict):
                    if 'value' in value:
                        value = value['value']
                    elif 'type' in value:
                        # Skip just the type info, look for actual value
                        value = str(value)
                col_values.append(value)

            columns.append({
                f"col{idx + 1}": col_name,
                "values": col_values
            })

        return {"values": columns}