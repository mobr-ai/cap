import logging
from collections import Counter
from typing import Any

from cap.services.vega.value_util import DataRow

logger = logging.getLogger(__name__)

class VegaChartConverter:
    @classmethod
    def _convert_bar_chart(cls, data: Any, user_query: str) -> dict[str, Any]:
        """Convert data to bar chart format."""
        if isinstance(data, list) and len(data) > 0:
            first_item = data[0]
            keys = cls._all_keys(data)

            # Parse coordinate assignments from user query
            coordinate_map = cls._parse_coordinate_assignments(user_query, data)
            field_assignments = cls._apply_coordinate_mapping(data, coordinate_map)

            # Determine category (x-axis) and value (y-axis) keys
            # Priority: user-specified coordinates > heuristic detection
            category_key = field_assignments.get('x')
            value_key = field_assignments.get('y')

            # Fallback to heuristic if not specified
            if not category_key:
                category_candidates = cls._get_x_candidates(first_item, keys)
                category_key = next((k for k in keys if k.lower() in [c.lower() for c in category_candidates]), keys[0])

            # Value field is typically numeric - find first numeric field that's not the category
            if not value_key:
                for k in keys:
                    if k != category_key and cls._is_numeric_field(data, k):
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
                    continue

                try:
                    values.append({
                        "category": cls._format_display_value(cat_val, category_key),
                        "amount": float(amt_val)
                    })
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping bar chart entry: {e}")
                    continue

            return {"values": values}

        return {"values": []}

    @classmethod
    def _convert_pie_chart(cls, data: Any, user_query: str) -> dict[str, Any]:
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
            keys = cls._all_keys(data)

            # Find category and value keys
            category_key = next((k for k in keys if k.lower() in ['category', 'label', 'name', 'group']), keys[0])
            value_key = next((k for k in keys if k != category_key), keys[-1])

            values = []
            for item in data:
                cat_val = item.get(category_key, "")
                val = item.get(value_key, 0)
                if isinstance(val, dict):
                    continue

                try:
                    values.append({
                        "category": cls._format_display_value(cat_val, category_key),
                        "value": float(val)
                    })
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping pie chart entry: {e}")
                    continue

            return {"values": values}

        return {"values": []}

    @classmethod
    def _detect_repetition_pattern(cls, data: list[DataRow], x_key: str) -> int:
        """Detect if x values repeat consistently, indicating multiple series in one variable."""
        if len(data) < 2:
            return 1

        x_values = [
            str(cls._format_x_value(item.get(x_key), x_key))
            for item in data
        ]
        x_counts = Counter(x_values)
        unique_counts = set(x_counts.values())

        # Consistent repetition pattern exists if all x values repeat the same number of times
        if len(unique_counts) == 1 and list(unique_counts)[0] > 1:
            return list(unique_counts)[0]

        return 1

    @classmethod
    def _extract_series_labels(
        cls,
        data: list[DataRow],
        x_key: str,
        series_keys: list[str],
        repetition_count: int,
        ) -> list[str]:
        """Extract series labels from repeating patterns in non-series columns."""
        if repetition_count <= 1:
            return []

        # Find columns that could contain series identifiers
        # (not x-axis, not y-axis/series values)
        candidate_keys = [
            k for k in cls._all_keys(data)
            if k != x_key and k not in series_keys
        ]

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
                labels.append(cls._abbreviate_label(label_str))

                if len(labels) == repetition_count:
                    break

        # Fill in any missing labels
        while len(labels) < repetition_count:
            labels.append(f"Series {len(labels)+1}")

        return labels


    @classmethod
    def _is_hour_field(cls, data: list[DataRow], key: str) -> bool:
        """Return True for fields that represent hour-of-day buckets, not dates."""
        key_lower = key.lower()
        if key_lower not in {"hour", "hours", "hourofday", "hour_of_day", "txhour", "tx_hour"}:
            return False

        for item in data:
            if not isinstance(item, dict) or item.get(key) is None:
                continue

            raw = cls._extract_raw_value(item.get(key))
            raw_str = str(raw).strip()

            # Accept 0-23, 00-23, and labels already rendered as HH:00 / HHh.
            if raw_str.endswith(":00"):
                raw_str = raw_str[:-3]
            if raw_str.endswith("h"):
                raw_str = raw_str[:-1]

            try:
                hour = int(raw_str)
            except (TypeError, ValueError):
                return False

            if hour < 0 or hour > 23:
                return False

        return True

    @classmethod
    def _format_hour_value(cls, value: Any) -> int | str:
        """Return hour buckets as numbers so Vega-Lite uses a quantitative x scale.

        Do not return strings like "00" or "00:00" here. Depending on the
        renderer/spec inference, those strings can be treated as temporal labels and
        disappear or render as calendar years/months.
        """
        raw = cls._extract_raw_value(value)
        raw_str = str(raw).strip()

        if raw_str.endswith(":00"):
            raw_str = raw_str[:-3]
        if raw_str.endswith("h"):
            raw_str = raw_str[:-1]

        try:
            return int(raw_str)
        except (TypeError, ValueError):
            return str(raw)

    @classmethod
    def _convert_line_chart(cls, data: Any, user_query: str) -> dict[str, Any]:
        """Convert data to line chart format with multi-series support."""
        if not isinstance(data, list) or len(data) == 0:
            logger.warning("cant serialize trend if it is not a list")
            return {"values": []}

        first_item = data[0]
        keys = cls._all_keys(data)

        # Parse coordinate assignments from user query
        coordinate_map = cls._parse_coordinate_assignments(user_query, data)
        field_assignments = cls._apply_coordinate_mapping(data, coordinate_map)

        # Determine x-axis field (typically time-based or sequential)
        # Priority: user-specified coordinates > heuristic detection
        x_key = field_assignments.get('x')

        if not x_key:
            x_candidates = cls._get_x_candidates(first_item, keys)
            x_key = next((k for k in keys if k.lower() in [c.lower() for c in x_candidates]), keys[0])

        # All other numeric fields are series (or user-specified y)
        series_keys = []
        y_key = field_assignments.get('y')

        if y_key:
            # User specified y coordinate, use that as the primary series
            series_keys = [y_key]
        else:
            # Auto-detect: all numeric fields except x_key
            for k in keys:
                if k != x_key and cls._is_numeric_field(data, k):
                    series_keys.append(k)

        # If no series keys found, skip this conversion
        if not series_keys:
            logger.warning("No numeric series found in data for line chart")
            return {"values": []}

        # Build line chart data with series index
        values = []
        x_is_hour = cls._is_hour_field(data, x_key)
        repetition_count = cls._detect_repetition_pattern(data, x_key)

        if repetition_count > 1 and len(series_keys) == 1:
            # Single variable contains multiple series via repetition
            for idx, item in enumerate(data):
                series_idx = idx % repetition_count
                raw_x = item.get(x_key)
                x_display = cls._format_hour_value(raw_x) if x_is_hour else cls._format_x_value(raw_x, x_key)
                y_val = cls._extract_y_value(item.get(series_keys[0]))

                if y_val is not None:
                    try:
                        values.append({"x": x_display, "y": float(y_val), "c": series_idx})
                    except Exception as e:
                        logger.warning(f"Failed to build series {series_idx}: {e}")
        else:
            # Multiple variables each represent a series
            for item in data:
                raw_x = item.get(x_key)
                x_display = cls._format_hour_value(raw_x) if x_is_hour else cls._format_x_value(raw_x, x_key)

                for series_idx, series_key in enumerate(series_keys):
                    y_val = cls._extract_y_value(item.get(series_key))

                    if y_val is not None:
                        try:
                            values.append({"x": x_display, "y": float(y_val), "c": series_idx})
                        except Exception as e:
                            logger.warning(f"Failed to build series {series_idx}: {e}")

        # Extract series labels if multiple series detected
        series_labels = []
        label_key = None

        if repetition_count > 1 and len(series_keys) == 1:
            candidate_keys = [
                k for k in cls._all_keys(data)
                if k != x_key and k not in series_keys
            ]
            if candidate_keys:
                label_key = candidate_keys[0]

            series_labels = cls._extract_series_labels(
                data, x_key, series_keys, repetition_count
            )

        elif len(series_keys) > 1:
            series_labels = [
                cls._format_column_name(series_key)
                for series_key in series_keys
            ]

        line_chart = {
            "values": values,
            "_series_labels": series_labels if series_labels else None,
            "_label_key": label_key,  # Which column was used for series labels
            "_x_key": x_key,  # X-axis column
            "_y_keys": series_keys,  # Y-axis column(s)
            "_field_types": {
                "x": "quantitative" if x_is_hour else ("temporal" if cls._is_date_field(data, x_key) else "nominal"),
                "y": "quantitative",
                "color": "nominal",
            },
            "_sort_fields": {
                "x": None,
            },
            "_axis_format": {
                "x": None if x_is_hour else None,
            },
        }
        logger.debug(f"converted to line chart: {line_chart}")
        return line_chart

    @classmethod
    def _convert_scatter_chart(cls, data: Any, user_query: str) -> dict[str, Any]:
        """Convert data to scatter chart format."""
        if not isinstance(data, list) or len(data) == 0:
            return {"values": []}

        keys = cls._all_keys(data)

        # Parse coordinate assignments from user query
        coordinate_map = cls._parse_coordinate_assignments(user_query, data)
        field_assignments = cls._apply_coordinate_mapping(data, coordinate_map)

        # Determine x and y keys
        # Priority: user-specified coordinates > heuristic detection
        x_key = field_assignments.get('x')
        y_key = field_assignments.get('y')

        # Find numeric fields for fallback
        numeric_keys = [k for k in keys if cls._is_numeric_field(data, k)]

        # Fallback to heuristic if coordinates not specified
        used_keys = [k for k in [x_key, y_key] if k is not None]
        available_numeric = [k for k in numeric_keys if k not in used_keys]

        if not x_key and available_numeric:
            x_key = available_numeric[0]
            available_numeric = available_numeric[1:]

        if not y_key and available_numeric:
            y_key = available_numeric[0]

        if not (x_key and y_key):
            logger.warning(f"Need at least 2 numeric fields for scatter chart. Found: x={x_key}, y={y_key}")
            return {"values": []}

        # Optional: find category field for coloring (from query or heuristic)
        category_key = field_assignments.get('color')
        if not category_key:
            # Heuristic: first non-numeric, non-coordinate field
            for k in keys:
                if k not in [x_key, y_key] and not cls._is_numeric_field(data, k):
                    category_key = k
                    break

        values = []
        for item in data:
            x_val = cls._extract_y_value(item.get(x_key))
            y_val = cls._extract_y_value(item.get(y_key))

            if x_val is not None and y_val is not None:
                try:
                    point: dict[str, Any] = {"x": float(x_val), "y": float(y_val)}
                    if category_key:
                        cat_val = item.get(category_key, "")
                        if isinstance(cat_val, dict):
                            cat_val = cat_val.get('value', str(cat_val))

                        point["category"] = cls._format_display_value(cat_val, category_key)
                    values.append(point)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping scatter point: {e}")
                    continue

        # Format column names for metadata
        formatted_columns = [
            cls._format_column_name(x_key),
            cls._format_column_name(y_key)
        ]
        if category_key:
            formatted_columns.append(cls._format_column_name(category_key))

        return {
            "values": values,
            "_x_key": x_key,
            "_y_key": y_key,
            "_category_key": category_key,
            "_columns": formatted_columns
        }

    @classmethod
    def _convert_bubble_chart(cls, data: Any, user_query: str) -> dict[str, Any]:
        """Convert data to bubble chart format (x, y, size)."""
        if not isinstance(data, list) or len(data) == 0:
            return {"values": []}

        keys = cls._all_keys(data)

        # Parse coordinate assignments from user query
        coordinate_map = cls._parse_coordinate_assignments(user_query, data)
        field_assignments = cls._apply_coordinate_mapping(data, coordinate_map)

        # Determine x, y, and size keys
        # Priority: user-specified coordinates > heuristic detection
        x_key = field_assignments.get('x')
        y_key = field_assignments.get('y')
        size_key = field_assignments.get('size')

        # Find numeric fields for fallback
        numeric_keys = [k for k in keys if cls._is_numeric_field(data, k)]

        # Filter out already assigned keys from numeric_keys for fallback selection
        used_keys = [k for k in [x_key, y_key, size_key] if k is not None]
        available_numeric = [k for k in numeric_keys if k not in used_keys]

        # Fallback to heuristic if coordinates not specified
        if not x_key and available_numeric:
            x_key = available_numeric[0]
            available_numeric = available_numeric[1:]

        if not y_key and available_numeric:
            y_key = available_numeric[0]
            available_numeric = available_numeric[1:]

        if not size_key and available_numeric:
            size_key = available_numeric[0]
            available_numeric = available_numeric[1:]

        # Validate we have at least 3 numeric fields
        if not (x_key and y_key and size_key):
            logger.warning(f"Need at least 3 numeric fields for bubble chart. Found: x={x_key}, y={y_key}, size={size_key}")
            return {"values": []}

        # Optional: find category/label field (from query or heuristic)
        label_key = field_assignments.get('color')  # Color can be used as label
        if not label_key:
            # Heuristic: first non-numeric field
            for k in keys:
                if k not in [x_key, y_key, size_key] and not cls._is_numeric_field(data, k):
                    label_key = k
                    break

        values = []
        for item in data:
            x_val = cls._extract_y_value(item.get(x_key))
            y_val = cls._extract_y_value(item.get(y_key))
            size_val = cls._extract_y_value(item.get(size_key))

            if x_val is not None and y_val is not None and size_val is not None:
                try:
                    bubble: dict[str, Any] = {
                        "x": float(x_val),
                        "y": float(y_val),
                        "size": float(size_val)
                    }
                    if label_key:
                        label_val = item.get(label_key, "")
                        if isinstance(label_val, dict):
                            label_val = label_val.get('value', str(label_val))

                        bubble["label"] = cls._format_display_value(label_val, label_key)
                    values.append(bubble)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping bubble: {e}")
                    continue

        # Format column names for metadata
        formatted_columns = [
            cls._format_column_name(x_key),
            cls._format_column_name(y_key),
            cls._format_column_name(size_key)
        ]
        if label_key:
            formatted_columns.append(cls._format_column_name(label_key))

        return {
            "values": values,
            "_x_key": x_key,
            "_y_key": y_key,
            "_size_key": size_key,
            "_label_key": label_key,
            "_columns": formatted_columns
        }

    @classmethod
    def _convert_treemap(cls, data: Any, user_query: str) -> dict[str, Any]:
        """Convert data to treemap format (hierarchical structure)."""
        if not isinstance(data, list) or len(data) == 0:
            return {"values": []}

        keys = cls._all_keys(data)

        # Find name/label and size fields
        name_key = next((k for k in keys if k.lower() in ['name', 'label', 'category', 'group', 'policyid', 'policy', 'token']), keys[0])

        # Find numeric field for size - use centralized numeric detection
        size_key = None
        for k in keys:
            if k != name_key and cls._is_numeric_field(data, k):
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

            size_val = cls._extract_y_value(item.get(size_key))

            if size_val is not None:
                try:
                    node = {
                        "name": cls._format_display_value(name_val, name_key),
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
            cls._format_column_name(name_key),
            cls._format_column_name(size_key)
        ]
        if parent_key:
            formatted_columns.append(cls._format_column_name(parent_key))

        return {
            "values": values,
            "_name_key": name_key,
            "_size_key": size_key,
            "_parent_key": parent_key,
            "_columns": formatted_columns
        }

    @classmethod
    def _convert_heatmap(cls, data: Any, user_query: str) -> dict[str, Any]:
        """Convert data to heatmap format (x, y, value)."""
        if not isinstance(data, list) or len(data) == 0:
            return {"values": []}

        keys = cls._all_keys(data)

        coordinate_map = cls._parse_coordinate_assignments(user_query, data)
        field_assignments = cls._apply_coordinate_mapping(data, coordinate_map)

        x_key = field_assignments.get("x")
        y_key = field_assignments.get("y")
        value_key = field_assignments.get("z")

        categorical_keys, numeric_keys = cls._classify_fields(data)

        used_keys = [k for k in [x_key, y_key, value_key] if k is not None]
        available_categorical = [k for k in categorical_keys if k not in used_keys]
        available_numeric = [k for k in numeric_keys if k not in used_keys]

        if not x_key and available_categorical:
            x_key = available_categorical[0]
            available_categorical = available_categorical[1:]

        if not y_key and available_categorical:
            y_key = available_categorical[0]

        if not value_key and available_numeric:
            value_key = available_numeric[0]
        elif not value_key and keys:
            value_key = keys[-1]

        if not (x_key and y_key and value_key):
            logger.warning(
                f"Need x, y, and value fields for heatmap. Found: x={x_key}, y={y_key}, value={value_key}"
            )
            return {"values": []}

        x_is_temporal = cls._is_date_field(data, x_key)
        y_is_temporal = cls._is_date_field(data, y_key)

        values = []

        for item in data:
            x_val = item.get(x_key, "")
            if isinstance(x_val, dict):
                x_val = x_val.get("value", str(x_val))

            y_val = item.get(y_key, "")
            if isinstance(y_val, dict):
                y_val = y_val.get("value", str(y_val))

            heat_val = cls._extract_y_value(item.get(value_key))

            if heat_val is None:
                continue

            try:
                x_raw = str(x_val)
                y_raw = str(y_val)

                x_dt = cls._parse_date_value(x_raw) if x_is_temporal else None
                y_dt = cls._parse_date_value(y_raw) if y_is_temporal else None

                x_label = x_dt.strftime("%d %b %Hh") if x_dt else x_raw
                y_label = y_dt.strftime("%d %b %Hh") if y_dt else y_raw

                values.append({
                    "x": x_label,
                    "y": y_label,
                    "x_raw": x_raw,
                    "y_raw": y_raw,
                    "value": float(heat_val),
                })

            except (ValueError, TypeError) as e:
                logger.warning(f"Skipping heatmap cell: {e}")
                continue

        formatted_columns = [
            cls._format_column_name(x_key),
            cls._format_column_name(y_key),
            cls._format_column_name(value_key),
        ]

        return {
            "values": values,
            "_x_key": x_key,
            "_y_key": y_key,
            "_value_key": value_key,
            "_columns": formatted_columns,
            "_field_types": {
                "x": "nominal",
                "y": "nominal",
                "value": "quantitative",
            },
            "_sort_fields": {
                "x": "x_raw" if x_is_temporal else None,
                "y": "y_raw" if y_is_temporal else None,
            },
        }
