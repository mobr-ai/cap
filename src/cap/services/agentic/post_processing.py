from typing import Any

from cap.federated.models import PostProcessingConfig


def apply_post_processing(
    kv_results: dict[str, Any] | None,
    post_processing: PostProcessingConfig | None,
) -> dict[str, Any] | None:
    if not kv_results or not post_processing:
        return kv_results

    data = kv_results.get("data")

    if isinstance(data, dict):
        rows = [data]
        original_was_dict = True
    elif isinstance(data, list):
        rows = [row for row in data if isinstance(row, dict)]
        original_was_dict = False
    else:
        return kv_results

    if not rows:
        return kv_results

    processed_rows = _apply_to_rows(rows, post_processing)

    kv_results["data"] = processed_rows[0] if original_was_dict and len(processed_rows) == 1 else processed_rows
    kv_results["count"] = len(processed_rows)

    metadata = kv_results.setdefault("metadata", {})
    metadata["post_processing"] = post_processing.model_dump()

    return kv_results


def _apply_to_rows(
    rows: list[dict[str, Any]],
    config: PostProcessingConfig,
) -> list[dict[str, Any]]:
    rows = [dict(row) for row in rows]

    if config.sort_by:
        rows.sort(key=lambda row: _sort_value(row.get(config.sort_by)))

    if config.type == "cumulative_sum":
        rows = _cumulative_sum(rows, config)

    elif config.type == "cumulative_count":
        rows = _cumulative_count(rows, config)

    elif config.type == "ratio":
        rows = _ratio(rows, config)

    elif config.type == "percentage":
        rows = _percentage(rows, config)

    elif config.type == "derived_field":
        rows = _derived_field(rows, config)

    else:
        return rows

    return _project_post_processed_fields(rows, config)


def _cumulative_sum(
    rows: list[dict[str, Any]],
    config: PostProcessingConfig,
) -> list[dict[str, Any]]:
    if len(config.source_fields) != 1:
        return rows

    source_field = config.source_fields[0]
    running_total = 0.0
    all_integral = True

    for row in rows:
        value = _to_number(row.get(source_field))
        if value is None:
            value = 0

        if not float(value).is_integer():
            all_integral = False

        running_total += value
        row[config.target_field] = int(running_total) if all_integral else running_total

    return rows


def _cumulative_count(
    rows: list[dict[str, Any]],
    config: PostProcessingConfig,
) -> list[dict[str, Any]]:
    if not config.source_fields:
        running_total = 0
        for row in rows:
            running_total += 1
            row[config.target_field] = running_total
        return rows

    source_field = config.source_fields[0]
    seen: set[Any] = set()

    for row in rows:
        value = _unwrap(row.get(source_field))
        if value is not None:
            seen.add(value)
        row[config.target_field] = len(seen)

    return rows


def _ratio(
    rows: list[dict[str, Any]],
    config: PostProcessingConfig,
) -> list[dict[str, Any]]:
    if len(config.source_fields) != 2:
        return rows

    numerator_field, denominator_field = config.source_fields

    for row in rows:
        numerator = _to_number(row.get(numerator_field))
        denominator = _to_number(row.get(denominator_field))

        if numerator is None or denominator in (None, 0):
            row[config.target_field] = None
        else:
            row[config.target_field] = numerator / denominator

    return rows


def _percentage(
    rows: list[dict[str, Any]],
    config: PostProcessingConfig,
) -> list[dict[str, Any]]:
    if len(config.source_fields) != 2:
        return rows

    numerator_field, denominator_field = config.source_fields

    for row in rows:
        numerator = _to_number(row.get(numerator_field))
        denominator = _to_number(row.get(denominator_field))

        if numerator is None or denominator in (None, 0):
            row[config.target_field] = None
        else:
            row[config.target_field] = (numerator / denominator) * 100

    return rows


def _derived_field(
    rows: list[dict[str, Any]],
    config: PostProcessingConfig,
) -> list[dict[str, Any]]:
    """
    Minimal safe derived field support.

    For now, this copies the first source field into target_field.
    Do not eval arbitrary expressions from the LLM here.
    Add explicit derived operations later if needed.
    """
    if len(config.source_fields) != 1:
        return rows

    source_field = config.source_fields[0]

    for row in rows:
        row[config.target_field] = row.get(source_field)

    return rows


def _unwrap(value: Any) -> Any:
    if isinstance(value, dict):
        return value.get("value")
    return value


def _to_number(value: Any) -> float | None:
    value = _unwrap(value)

    if isinstance(value, bool):
        return None

    if isinstance(value, int | float):
        return float(value)

    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None

    return None


def _sort_value(value: Any) -> Any:
    value = _unwrap(value)

    if value is None:
        return ""

    return value

def _project_post_processed_fields(
    rows: list[dict[str, Any]],
    config: PostProcessingConfig,
) -> list[dict[str, Any]]:
    keep_fields = set()

    if config.sort_by:
        keep_fields.add(config.sort_by)

    keep_fields.add(config.target_field)

    # Keep common grouping/category fields that may be needed by charts.
    # Do not keep source numeric fields by default.
    for row in rows:
        for key, value in row.items():
            if key in keep_fields:
                continue

            key_lower = key.lower()
            if key_lower in {"category", "label", "name", "metric", "source"}:
                keep_fields.add(key)

    return [
        {key: row.get(key) for key in row.keys() if key in keep_fields}
        for row in rows
    ]