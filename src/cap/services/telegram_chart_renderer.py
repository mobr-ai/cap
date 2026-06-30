import io
import logging
import urllib.request
from urllib.parse import urlparse

import hashlib
import os
import secrets
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from PIL import Image, ImageEnhance, ImageDraw, ImageFont

from cap.database.model import TelegramRenderedImage, User
from cap.services.vega.facade import VegaConverter

logger = logging.getLogger(__name__)

DEFAULT_TELEGRAM_RENDER_DIR = "/var/lib/cap/telegram-renders"
DEFAULT_PUBLIC_BASE_URL = "http://localhost:8000"
DEFAULT_CAP_LOGO_URL = "https://cap.mobr.ai/icons/logo.png"

def telegram_render_dir() -> Path:
    return Path(
        os.getenv("TELEGRAM_RENDER_DIR", DEFAULT_TELEGRAM_RENDER_DIR)
    ).resolve()


def _public_base_url() -> str:
    return (os.getenv("PUBLIC_BASE_URL") or DEFAULT_PUBLIC_BASE_URL).rstrip("/")


def _cap_logo_source() -> str:
    """
    Supports both:
    - CAP_LOGO_URL=https://cap.mobr.ai/icons/logo.png
    - CAP_LOGO_PATH=/local/path/logo.png

    Defaults to the hosted CAP logo.
    """
    return (
        os.getenv("CAP_LOGO_URL")
        or os.getenv("CAP_LOGO_PATH")
        or DEFAULT_CAP_LOGO_URL
    ).strip()


def _load_cap_logo() -> Image.Image | None:
    source = _cap_logo_source()

    if not source:
        return None

    try:
        parsed = urlparse(source)

        if parsed.scheme in {"http", "https"}:
            request = urllib.request.Request(
                source,
                headers={"User-Agent": "CAP-Telegram-Renderer/1.0"},
            )

            with urllib.request.urlopen(request, timeout=8) as response:
                data = response.read()

            return Image.open(io.BytesIO(data)).convert("RGBA")

        logo_path = Path(source)

        if not logo_path.exists():
            logger.warning("CAP logo path does not exist: %s", source)
            return None

        return Image.open(logo_path).convert("RGBA")

    except Exception:
        logger.exception("Failed to load CAP logo for Telegram watermark")
        return None


def _image_ttl_days() -> int:
    return int(os.getenv("TELEGRAM_RENDER_TTL_DAYS", "2"))


def _ensure_dir() -> Path:
    render_dir = telegram_render_dir()
    render_dir.mkdir(parents=True, exist_ok=True)
    return render_dir


def _watermark_footer_text() -> str:
    return os.getenv("CAP_WATERMARK_TEXT", "https://cap.mobr.ai").strip()


def _watermark_png(image_path: Path) -> None:
    logo = _load_cap_logo()

    base = Image.open(image_path).convert("RGBA")
    layer = Image.new("RGBA", base.size, (255, 255, 255, 0))

    # Optional large background logo
    if logo is not None:
        max_w = int(base.width)
        max_h = int(base.height)

        scale = min(max_w / logo.width, max_h / logo.height)
        new_size = (
            max(1, int(logo.width * scale)),
            max(1, int(logo.height * scale)),
        )
        logo = logo.resize(new_size, Image.LANCZOS)

        alpha = logo.getchannel("A")
        alpha = ImageEnhance.Brightness(alpha).enhance(0.5)
        logo.putalpha(alpha)

        x = (base.width - logo.width) // 2
        y = (base.height - logo.height) // 2
        layer.paste(logo, (x, y), logo)

    # Bottom-left text
    text = _watermark_footer_text()
    if text:
        draw = ImageDraw.Draw(layer)

        # reasonable size for 1200x760 images
        font_size = max(18, int(base.width * 0.018))

        try:
            # Try a common system font first
            font = ImageFont.truetype("DejaVuSans.ttf", font_size)
        except Exception:
            font = ImageFont.load_default()

        padding_x = 24
        padding_y = 18

        bbox = draw.textbbox((0, 0), text, font=font)
        text_w = bbox[2] - bbox[0]
        text_h = bbox[3] - bbox[1]

        x = padding_x
        y = base.height - text_h - padding_y

        # subtle white backing to improve readability
        bg_pad_x = 10
        bg_pad_y = 6
        draw.rounded_rectangle(
            (
                x - bg_pad_x,
                y - bg_pad_y,
                x + text_w + bg_pad_x,
                y + text_h + bg_pad_y,
            ),
            radius=8,
            fill=(255, 255, 255, 150),
        )

        # dark semi-transparent text
        draw.text(
            (x, y),
            text,
            font=font,
            fill=(55, 55, 55, 180),
        )

    out = Image.alpha_composite(base, layer).convert("RGB")
    out.save(image_path, "PNG", optimize=True)


def _axis_title(value: Any | None) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    return VegaConverter._format_column_name(text)


def _semantic_label(
    vega: dict[str, Any],
    semantic_key: str,
    fallback_key: str | None = None,
    fallback: str | None = None,
) -> str | None:
    """
    Returns a human-readable label for semantic metadata keys such as:
      _x_key, _y_key, _size_key, _label_key, _color_key
    """
    value = vega.get(semantic_key) or fallback_key or fallback

    if value is None:
        return None

    return _axis_title(value)


def _columns(vega: dict[str, Any]) -> list[str]:
    columns = vega.get("_columns") or []
    return columns if isinstance(columns, list) else []


def _unique_in_order(values: list[dict[str, Any]], key: str) -> list[Any]:
    seen = set()
    out = []

    for row in values:
        if not isinstance(row, dict) or key not in row:
            continue

        value = row.get(key)

        if value in seen:
            continue

        seen.add(value)
        out.append(value)

    return out


def _format_label(value: Any) -> str:
    return VegaConverter._format_column_name(str(_scalar(value)))


def _is_zero_based_index_series(series_values: list[Any]) -> bool:
    try:
        ints = [int(value) for value in series_values]
    except Exception:
        return False

    return sorted(ints) == list(range(len(ints)))


def _infer_x_column_from_metadata(
    *,
    vega: dict[str, Any],
    values: list[dict[str, Any]],
) -> str | None:
    columns = _columns(vega)

    if not columns:
        return None

    explicit_x = vega.get("_x_key")
    if explicit_x:
        explicit_x_formatted = _axis_title(explicit_x)
        for col in columns:
            if col == explicit_x or col == explicit_x_formatted:
                return col

    # Common timestamp / date column names.
    for candidate in (
        "ts",
        "timestamp",
        "time",
        "date",
        "day",
        "hour",
        "datetime",
        "block_time",
        "created_at",
    ):
        for col in columns:
            normalized = str(col).replace(" ", "_").lower()
            if normalized == candidate:
                return col

    # Fallback: if x values look like datetimes, prefer the first metadata
    # column that looks temporal.
    sample_x = None
    for row in values:
        if isinstance(row, dict) and row.get("x") is not None:
            sample_x = row.get("x")
            break

    if sample_x is not None:
        sample_text = str(sample_x)
        if "T" in sample_text or "-" in sample_text or ":" in sample_text:
            for col in columns:
                normalized = str(col).lower()
                if normalized in {"ts", "time", "date"} or "time" in normalized or "date" in normalized:
                    return col

    return columns[0]


def _series_label_map(
    *,
    vega: dict[str, Any],
    values: list[dict[str, Any]],
    series_key: str = "c",
) -> dict[Any, str]:
    """
    Maps normalized series ids like c=0, c=1, c=2 back to semantic labels.

    Example:
      values:
        {"x": "...", "y": 0.23, "c": 0}
        {"x": "...", "y": 0.22, "c": 1}

      metadata.columns:
        ["Source", "Ts", "Price", "Ema 20", "Bb Upper", "Bb Middle", "Bb Lower"]

      output:
        {
          0: "Price",
          1: "Ema 20",
          2: "Bb Upper",
          3: "Bb Middle",
          4: "Bb Lower",
        }
    """
    series_values = _unique_in_order(values, series_key)

    if not series_values:
        return {}

    # 1) Prefer explicit labels if the converter already provides them.
    explicit = (
        vega.get("_series_labels")
        or vega.get("_series_label_map")
        or vega.get("_c_labels")
        or vega.get("_color_labels")
    )

    if isinstance(explicit, dict):
        return {
            raw: str(
                explicit.get(raw)
                or explicit.get(str(raw))
                or explicit.get(int(raw)) if str(raw).isdigit() else explicit.get(str(raw))
                or raw
            )
            for raw in series_values
        }

    if isinstance(explicit, list) and len(explicit) >= len(series_values):
        return {
            raw: _format_label(explicit[idx])
            for idx, raw in enumerate(series_values)
        }

    # 2) Then use _y_keys if available.
    y_keys = vega.get("_y_keys")
    if isinstance(y_keys, list) and len(y_keys) >= len(series_values):
        return {
            raw: _format_label(y_keys[idx])
            for idx, raw in enumerate(series_values)
        }

    # 3) Infer from metadata.columns.
    columns = _columns(vega)
    if not columns:
        return {raw: str(raw) for raw in series_values}

    if not _is_zero_based_index_series(series_values):
        return {raw: _format_label(raw) for raw in series_values}

    n = len(series_values)
    x_column = _infer_x_column_from_metadata(vega=vega, values=values)

    candidate_labels: list[str] = []

    if x_column and x_column in columns:
        after_x = columns[columns.index(x_column) + 1:]
        if len(after_x) >= n:
            candidate_labels = after_x[:n]

    # Fallback for cases like:
    # ["Source", "Ts", "Price", "Ema 20", "Bb Upper", "Bb Middle", "Bb Lower"]
    # where Source is a dimension, Ts is x, and the final 5 columns are the series.
    if len(candidate_labels) < n and len(columns) >= n:
        candidate_labels = columns[-n:]

    if len(candidate_labels) < n:
        return {raw: str(raw) for raw in series_values}

    return {
        raw: _format_label(candidate_labels[idx])
        for idx, raw in enumerate(series_values)
    }


def _series_name(
    value: Any,
    label_map: dict[Any, str],
) -> str:
    if value in label_map:
        return label_map[value]

    value_text = str(value)

    if value_text in label_map:
        return label_map[value_text]

    try:
        value_int = int(value)
        if value_int in label_map:
            return label_map[value_int]
    except Exception:
        pass

    return value_text


def _axis_titles(
    result_type: str,
    vega: dict[str, Any],
    *,
    default_x: str | None = None,
    default_y: str | None = None,
) -> tuple[str | None, str | None]:
    columns = _columns(vega)

    x_key = vega.get("_x_key")
    y_key = vega.get("_y_key")

    if result_type == "line_chart":
        y_keys = vega.get("_y_keys") or []
        if not y_key and isinstance(y_keys, list) and len(y_keys) == 1:
            y_key = y_keys[0]
        elif not y_key and isinstance(y_keys, list) and len(y_keys) > 1:
            y_key = "value"

    if not x_key and len(columns) >= 1:
        x_key = columns[0]
    if not y_key and len(columns) >= 2:
        y_key = columns[1]

    return (
        _axis_title(x_key) or default_x,
        _axis_title(y_key) or default_y,
    )


def _layout(
    fig: go.Figure,
    title: str | None = None,
    *,
    x_title: str | None = None,
    y_title: str | None = None,
) -> go.Figure:
    fig.update_layout(
        title={
            "text": title or "",
            "x": 0.02,
            "xanchor": "left",
        },
        width=1200,
        height=760,
        margin={"l": 95, "r": 70, "t": 115, "b": 130},
        font={"size": 18},
        paper_bgcolor="white",
        plot_bgcolor="white",
    )

    if x_title:
        fig.update_xaxes(title_text=x_title, title_standoff=18, automargin=True)

    if y_title:
        fig.update_yaxes(title_text=y_title, title_standoff=18, automargin=True)

    return fig


def _table_to_dataframe(vega: dict[str, Any]) -> pd.DataFrame:
    columns = vega.get("_columns") or []
    values = vega.get("values") or []

    if not values:
        context = vega.get("context") or {}
        return pd.DataFrame([context]) if context else pd.DataFrame()

    max_len = max(len(col.get("values", [])) for col in values)
    rows = []
    for i in range(max_len):
        row = {}
        for idx, col in enumerate(values):
            label = columns[idx] if idx < len(columns) else f"col{idx + 1}"
            col_values = col.get("values", [])
            row[label] = col_values[i] if i < len(col_values) else ""
        rows.append(row)

    return pd.DataFrame(rows)


def _with_metadata_columns(
    payload: dict[str, Any],
    kv_results: dict[str, Any],
) -> dict[str, Any]:
    """
    format_kv strips internal _columns from Vega payloads and keeps display
    column names under metadata.columns. Put them back for Telegram rendering.
    """
    metadata = kv_results.get("metadata") or {}
    columns = metadata.get("columns") if isinstance(metadata, dict) else None

    if columns and "_columns" not in payload:
        payload = dict(payload)
        payload["_columns"] = columns

    if isinstance(metadata, dict):
        for source_key, target_key in (
            ("series_labels", "_series_labels"),
            ("series_label_map", "_series_label_map"),
            ("y_keys", "_y_keys"),
            ("x_key", "_x_key"),
            ("y_key", "_y_key"),
            ("size_key", "_size_key"),
            ("label_key", "_label_key"),
            ("color_key", "_color_key"),
        ):
            if source_key in metadata and target_key not in payload:
                payload = dict(payload)
                payload[target_key] = metadata[source_key]

    return payload


def _with_raw_axis_metadata(
    *,
    result_type: str,
    payload: dict[str, Any],
    raw_data: Any,
    user_query: str,
) -> dict[str, Any]:
    if not isinstance(raw_data, list) or not raw_data:
        return payload

    keys = VegaConverter._all_keys(raw_data)
    if not keys:
        return payload

    payload = dict(payload)
    payload.setdefault(
        "_columns",
        [VegaConverter._format_column_name(key) for key in keys],
    )

    if result_type not in {"bar_chart", "bubble_chart"}:
        return payload

    first_item = raw_data[0]
    coordinate_map = VegaConverter._parse_coordinate_assignments(user_query, raw_data)
    field_assignments = VegaConverter._apply_coordinate_mapping(raw_data, coordinate_map)

    x_key = field_assignments.get("x")
    y_key = field_assignments.get("y")

    if not x_key:
        x_candidates = VegaConverter._get_x_candidates(first_item, keys)
        x_candidate_names = {candidate.lower() for candidate in x_candidates}
        x_key = next(
            (key for key in keys if key.lower() in x_candidate_names),
            keys[0],
        )

    if not y_key:
        y_key = next(
            (
                key for key in keys
                if key != x_key and VegaConverter._is_numeric_field(raw_data, key)
            ),
            None,
        )


    if result_type == "bubble_chart":
        if not y_key:
            y_key = keys[-1] if len(keys) > 1 else keys[0]

    if result_type == "bubble_chart":
        size_key = field_assignments.get("size") or field_assignments.get("z")
        if not size_key:
            numeric_candidates = [
                key for key in keys
                if key not in {x_key, y_key}
                and VegaConverter._is_numeric_field(raw_data, key)
            ]
            size_key = numeric_candidates[0] if numeric_candidates else None

        if size_key:
            payload.setdefault("_size_key", size_key)

    payload.setdefault("_x_key", x_key)
    payload.setdefault("_y_key", y_key)

    return payload


def _extract_converted_vega_payload(
    kv_results: dict[str, Any],
) -> dict[str, Any] | None:
    """
    Accept already-converted Vega/widget payloads from all known paths.

    Supported shapes:
    - {"config": {"values": [...]}}
    - {"vega": {"values": [...]}}
    - {"data": {"values": [...]}}          <-- format_kv / Telegram stream path
    - {"data": {"config": {"values": [...]}}}
    - {"data": {"vega": {"values": [...]}}}
    """
    for key in ("config", "vega", "data"):
        payload = kv_results.get(key)
        if isinstance(payload, dict) and "values" in payload:
            return _with_metadata_columns(payload, kv_results)

    data = kv_results.get("data")
    if isinstance(data, dict):
        for key in ("config", "vega", "data"):
            payload = data.get(key)
            if isinstance(payload, dict) and "values" in payload:
                return _with_metadata_columns(payload, kv_results)

    return None


def _normalize_telegram_chart_payload(
    kv_results: dict[str, Any],
) -> tuple[str, dict[str, Any], str | None]:
    """
    Normalize Telegram chart payloads into the Vega shape expected by
    _figure_from_vega.

    This function must support both:
    1. Raw kv_results used by telegram_renderer_tests.py:
       {"result_type": "line_chart", "data": [...]}

    2. Stream/widget payload emitted by format_kv:
       {"result_type": "line_chart", "data": {"values": [...]}}

    The second case is the Telegram bot failure path. Do not send it back
    through VegaConverter, because it is already converted.
    """
    result_type = (
        kv_results.get("result_type")
        or kv_results.get("type")
        or kv_results.get("visualization_type")
        or "text"
    )

    title = (
        kv_results.get("title")
        or kv_results.get("user_query")
        or kv_results.get("nl_query")
    )

    message = kv_results.get("message")

    converted_payload = _extract_converted_vega_payload(kv_results)
    if converted_payload is not None:
        converted_payload = dict(converted_payload)
        if message and not converted_payload.get("values"):
            converted_payload["_message"] = message
        return result_type, converted_payload, title

    user_query = kv_results.get("user_query") or kv_results.get("nl_query") or ""

    normalized_raw = dict(kv_results)
    normalized_raw["result_type"] = result_type

    converted = VegaConverter.convert_to_vega_format(
        kv_results=normalized_raw,
        user_query=user_query,
    )

    converted = _with_raw_axis_metadata(
        result_type=result_type,
        payload=converted,
        raw_data=kv_results.get("data"),
        user_query=user_query,
    )

    if message and not converted.get("values"):
        converted = dict(converted)
        converted["_message"] = message

    return result_type, converted, title


def render_telegram_image(
    *,
    db,
    cap_user: User,
    telegram_user_id: int,
    telegram_chat_id: int | None,
    kv_results: dict[str, Any],
    absolute: bool = True,
) -> dict[str, Any] | None:
    """
    Renders table/bar/line/scatter/bubble/pie/heatmap/treemap to PNG.
    Returns a short-lived URL that the bot can send as photo/document.
    """

    result_type, vega, title = _normalize_telegram_chart_payload(kv_results)
    if result_type == "text":
        return None

    fig = _figure_from_vega(result_type, vega, title)

    render_dir = _ensure_dir()
    image_id = str(uuid.uuid4())
    filename = f"{image_id}.png"
    path = render_dir / filename

    fig.write_image(str(path), format="png", scale=2)
    _watermark_png(path)

    raw = path.read_bytes()
    etag = hashlib.sha256(raw).hexdigest()
    token = secrets.token_urlsafe(32)

    obj = TelegramRenderedImage(
        id=image_id,
        cap_user_id=cap_user.user_id,
        telegram_user_id=telegram_user_id,
        telegram_chat_id=telegram_chat_id,
        access_token=token,
        mime="image/png",
        bytes=len(raw),
        etag=etag,
        storage_path=filename,
        expires_at=datetime.now() + timedelta(days=_image_ttl_days()),
    )
    db.add(obj)
    db.commit()

    rel = f"/api/v1/telegram/image/{image_id}?t={token}"
    return {
        "url": f"{_public_base_url()}{rel}" if absolute else rel,
        "mime": "image/png",
        "bytes": len(raw),
        "expires_at": obj.expires_at.isoformat(),
    }


def _scalar(value: Any) -> Any:
    """
    Telegram renderer helper.

    SPARQL values can arrive as raw scalars, or as dicts such as:
    {"value": "123.4", "type": "literal", ...}
    {"ada": "0.17", "lovelace": "..."}
    """
    if isinstance(value, dict):
        for key in ("ada", "value", "lovelace"):
            if key in value:
                return value[key]
        return ""

    return value


def _number(value: Any, default: float | None = None) -> float | None:
    value = _scalar(value)

    if value is None or value == "":
        return default

    try:
        return float(value)
    except Exception:
        return default


def _first_existing(columns: list[str], candidates: list[str]) -> str | None:
    lower_to_real = {str(col).lower(): col for col in columns}

    for candidate in candidates:
        real = lower_to_real.get(candidate.lower())
        if real:
            return real

    return None


def _numeric_column(
    rows: list[dict[str, Any]],
    columns: list[str],
    *,
    exclude: set[str] | None = None,
) -> str | None:
    exclude = exclude or set()

    for col in columns:
        if col in exclude:
            continue

        for row in rows:
            if _number(row.get(col)) is not None:
                return col

    return None


def _empty_figure(title: str | None, message: str = "No results found") -> go.Figure:
    fig = go.Figure()

    fig.add_annotation(
        text=message,
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
        font={"size": 28},
    )

    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)

    fig.update_layout(
        title=title or "",
        width=1200,
        height=760,
        margin={"l": 50, "r": 50, "t": 90, "b": 50},
        font={"size": 18},
        paper_bgcolor="white",
        plot_bgcolor="white",
    )

    return fig


def _short_label(value: Any, max_len: int = 26) -> str:
    text = str(_scalar(value) or "")

    if len(text) <= max_len:
        return text

    return f"{text[:12]}…{text[-10:]}"


def _figure_from_vega(result_type: str, vega: dict[str, Any], title: str | None) -> go.Figure:
    title = _clean_chart_title(title)
    values = vega.get("values") or []

    if not values and result_type != "table":
        return _empty_figure(title, str(vega.get("_message") or "No results found"))

    if result_type == "table":
        df = _table_to_dataframe(vega).head(25)

        # Dynamic height calculation
        header_height = 45
        row_height = 38
        table_height = header_height + (len(df) * row_height)

        fig = go.Figure(
            data=[
                go.Table(
                    header={
                        "values": list(df.columns),
                        "align": "left",
                        "height": header_height,
                        "font": {"size": 18},
                    },
                    cells={
                        "values": [df[c].astype(str).tolist() for c in df.columns],
                        "align": "left",
                        "height": row_height,
                        "font": {"size": 16},
                    },
                )
            ]
        )

        fig.update_layout(
            title=title or "",
            width=1200,
            height=max(760, table_height + 120),
            margin={"l": 50, "r": 50, "t": 90, "b": 50},
            paper_bgcolor="white",
        )

        return fig

    if result_type == "bar_chart":
        raw_categories = [v.get("category") for v in values]

        if any(_is_datetime_like(value) for value in raw_categories):
            categories = _display_category_labels(raw_categories)
        else:
            category_map = _series_label_map(
                vega=vega,
                values=values,
                series_key="category",
            )

            categories = [
                _series_name(v.get("category"), category_map)
                for v in values
            ]

        fig = go.Figure(
            data=[
                go.Bar(
                    x=categories,
                    y=[v.get("amount") for v in values],
                )
            ]
        )

        x_title, y_title = _axis_titles(
            result_type,
            vega,
            default_x="Category",
            default_y="Amount",
        )

        fig = _layout(fig, title, x_title=x_title, y_title=y_title)

        # Keep bar-chart labels readable when there are many categories.
        fig.update_xaxes(
            tickangle=-45,
            automargin=True,
        )
        if len(categories) > 24:
            fig.update_xaxes(nticks=18)

        return fig

    if result_type == "pie_chart":
        category_map = _series_label_map(
            vega=vega,
            values=values,
            series_key="category",
        )

        labels = [
            _series_name(v.get("category"), category_map)
            for v in values
        ]

        fig = go.Figure(
            data=[
                go.Pie(
                    labels=labels,
                    values=[v.get("value") for v in values],
                )
            ]
        )

        return _layout(fig, title)

    if result_type == "line_chart":
        df = pd.DataFrame(values)
        fig = go.Figure()

        if "c" in df.columns:
            label_map = _series_label_map(
                vega=vega,
                values=values,
                series_key="c",
            )

            for c, group in df.groupby("c", sort=False):
                fig.add_trace(
                    go.Scatter(
                        x=group["x"],
                        y=group["y"],
                        mode="lines+markers",
                        name=_series_name(c, label_map),
                    )
                )
        else:
            trace_name = None

            y_keys = vega.get("_y_keys")
            if isinstance(y_keys, list) and len(y_keys) == 1:
                trace_name = _format_label(y_keys[0])
            elif vega.get("_y_key"):
                trace_name = _format_label(vega.get("_y_key"))

            fig.add_trace(
                go.Scatter(
                    x=df.get("x"),
                    y=df.get("y"),
                    mode="lines+markers",
                    name=trace_name,
                )
            )

        x_title, y_title = _axis_titles(result_type, vega, default_x="X", default_y="Value")
        return _layout(fig, title, x_title=x_title, y_title=y_title)

    if result_type == "scatter_chart":
        df = pd.DataFrame(values)
        fig = go.Figure()

        if "c" in df.columns:
            label_map = _series_label_map(
                vega=vega,
                values=values,
                series_key="c",
            )

            for c, group in df.groupby("c", sort=False):
                fig.add_trace(
                    go.Scatter(
                        x=group["x"],
                        y=group["y"],
                        mode="markers",
                        name=_series_name(c, label_map),
                    )
                )
        else:
            fig.add_trace(
                go.Scatter(
                    x=df.get("x"),
                    y=df.get("y"),
                    mode="markers",
                )
            )

        x_title, y_title = _axis_titles(result_type, vega, default_x="X", default_y="Y")
        return _layout(fig, title, x_title=x_title, y_title=y_title)

    if result_type == "bubble_chart":
        rows = [row for row in values if isinstance(row, dict)]
        if not rows:
            return _empty_figure(title, str(vega.get("_message") or "No results found"))

        columns = list(rows[0].keys())

        # IMPORTANT:
        # VegaConverter may output normalized rows:
        #   {"x": ..., "y": ..., "size": ...}
        # while metadata keeps original semantic keys:
        #   _x_key = "epochNumber", _y_key = "tps", _size_key = "avgFee"
        #
        # For plotting, use the actual keys present in each row.
        # For labels, use the semantic metadata keys when available.

        x_value_key = (
            "x" if "x" in columns
            else _first_existing(columns, ["epochNumber", "epoch", "timePeriod", "date", "day"])
            or columns[0]
        )

        y_value_key = (
            "y" if "y" in columns
            else _first_existing(columns, ["tps", "TPS", "value", "amount"])
            or _numeric_column(rows, columns, exclude={x_value_key})
        )

        size_value_key = (
            "size" if "size" in columns
            else "z" if "z" in columns
            else _first_existing(columns, ["avgFee", "averageFee", "fee", "totalTx", "count"])
            or _numeric_column(
                rows,
                columns,
                exclude={x_value_key, y_value_key} if y_value_key else {x_value_key},
            )
        )

        if not y_value_key:
            return _empty_figure(title, "Could not determine bubble chart Y values")

        x_values = [_scalar(row.get(x_value_key)) for row in rows]
        y_values = [_number(row.get(y_value_key)) for row in rows]

        raw_sizes = [
            _number(row.get(size_value_key), 1.0) if size_value_key else 1.0
            for row in rows
        ]

        # Avoid invisible bubbles when size values are small decimals,
        # for example avgFee ~= 0.30 ADA.
        numeric_sizes = [float(size or 1.0) for size in raw_sizes]
        min_size = min(numeric_sizes) if numeric_sizes else 1.0
        max_size = max(numeric_sizes) if numeric_sizes else 1.0

        if max_size == min_size:
            marker_sizes = [35 for _ in numeric_sizes]
        else:
            marker_sizes = [
                20 + ((size - min_size) / (max_size - min_size)) * 60
                for size in numeric_sizes
            ]

        x_title = _semantic_label(vega, "_x_key", x_value_key, "X") or "X"
        y_title = _semantic_label(vega, "_y_key", y_value_key, "Y") or "Y"
        size_title = _semantic_label(vega, "_size_key", size_value_key, "Bubble size") or "Bubble size"

        size_values = [
            _number(row.get(size_value_key), 1.0) if size_value_key else 1.0
            for row in rows
        ]

        hover_text = []
        for row, raw_size in zip(rows, size_values):
            parts = [
                f"{x_title}: {_scalar(row.get(x_value_key))}",
                f"{y_title}: {_scalar(row.get(y_value_key))}",
                f"{size_title}: {raw_size}",
            ]

            # Include remaining useful fields without duplicating x/y/size.
            for col in columns:
                if col in {x_value_key, y_value_key, size_value_key}:
                    continue

                value = row.get(col)
                if value is not None:
                    parts.append(f"{_axis_title(col) or col}: {_scalar(value)}")

            hover_text.append("<br>".join(parts))

        fig = go.Figure(
            data=[
                go.Scatter(
                    x=x_values,
                    y=y_values,
                    mode="markers",
                    marker={
                        "size": marker_sizes,
                        "sizemode": "diameter",
                        "opacity": 0.75,
                    },
                    text=hover_text,
                    hovertemplate="%{text}<extra></extra>",
                )
            ]
        )

        fig = _layout(fig, title, x_title=x_title, y_title=y_title)
        fig.add_annotation(
            text=f"Bubble size = {size_title}",
            x=0.99,
            y=0.99,
            xref="paper",
            yref="paper",
            showarrow=False,
            xanchor="right",
            yanchor="top",
            align="right",
            font={"size": 16},
            bgcolor="rgba(255,255,255,0.75)",
            bordercolor="rgba(80,80,80,0.25)",
            borderwidth=1,
            borderpad=6,
        )

        return fig

    if result_type == "heatmap":
        df = pd.DataFrame(values)
        pivot = df.pivot_table(index="y", columns="x", values="value", aggfunc="sum")
        fig = go.Figure(data=[go.Heatmap(z=pivot.values, x=list(pivot.columns), y=list(pivot.index))])
        x_title, y_title = _axis_titles(result_type, vega, default_x="X", default_y="Y")
        return _layout(fig, title, x_title=x_title, y_title=y_title)

    if result_type == "treemap":
        rows = [row for row in values if isinstance(row, dict)]
        if not rows:
            return _empty_figure(title, str(vega.get("_message") or "No results found"))

        columns = list(rows[0].keys())

        label_key = (
            _first_existing(
                columns,
                [
                    "label",
                    "name",
                    "category",
                    "policyId",
                    "policyID",
                    "policy_id",
                    "tokenName",
                    "assetName",
                ],
            )
            or columns[0]
        )

        value_key = (
            _first_existing(
                columns,
                [
                    "value",
                    "amount",
                    "mintCount",
                    "count",
                    "total",
                    "transfers",
                    "deployments",
                ],
            )
            or _numeric_column(rows, columns, exclude={label_key})
        )

        parent_key = _first_existing(columns, ["parent", "group", "categoryParent"])

        if not value_key:
            return _empty_figure(title, "Could not determine treemap values")

        labels = [_short_label(row.get(label_key)) for row in rows]
        parents = [
            _short_label(row.get(parent_key)) if parent_key and row.get(parent_key) else ""
            for row in rows
        ]
        numeric_values = [_number(row.get(value_key), 0.0) for row in rows]

        if not any(value and value > 0 for value in numeric_values):
            return _empty_figure(title, str(vega.get("_message") or "No positive values to plot"))

        fig = go.Figure(
            data=[
                go.Treemap(
                    labels=labels,
                    parents=parents,
                    values=numeric_values,
                    customdata=[_scalar(row.get(label_key)) for row in rows],
                    hovertemplate="%{customdata}<br>Value: %{value}<extra></extra>",
                )
            ]
        )

        return _layout(fig, title)

    raise ValueError(f"Unsupported Telegram render type: {result_type}")

def _parse_datetime_like(value: Any) -> pd.Timestamp | None:
    raw = _scalar(value)

    if raw is None or raw == "":
        return None

    text = str(raw).strip()

    # Avoid treating plain numbers like epoch numbers as dates.
    if text.replace(".", "", 1).isdigit():
        return None

    try:
        ts = pd.to_datetime(text, errors="coerce", utc=False)
    except Exception:
        return None

    if pd.isna(ts):
        return None

    return ts


def _is_datetime_like(value: Any) -> bool:
    return _parse_datetime_like(value) is not None


def _format_datetime_label(value: Any, *, granularity: str | None = None) -> str:
    ts = _parse_datetime_like(value)

    if ts is None:
        return str(_scalar(value) or "")

    # Remove timezone wrapper if pandas returns one.
    try:
        ts = ts.tz_localize(None)
    except Exception:
        pass

    if granularity == "year":
        return ts.strftime("%Y")

    if granularity == "month":
        return ts.strftime("%Y-%m")

    if granularity == "day":
        return ts.strftime("%Y-%m-%d")

    if granularity == "hour":
        return ts.strftime("%Y-%m-%d %H:%M")

    # Automatic compact formatting.
    if ts.hour == 0 and ts.minute == 0 and ts.second == 0:
        if ts.day == 1:
            return ts.strftime("%Y-%m")
        return ts.strftime("%Y-%m-%d")

    return ts.strftime("%Y-%m-%d %H:%M")


def _infer_time_granularity(values: list[Any]) -> str | None:
    timestamps = [_parse_datetime_like(value) for value in values]
    timestamps = [ts for ts in timestamps if ts is not None]

    if not timestamps:
        return None

    # Monthly series: most values are first day of month at midnight.
    month_like = [
        ts for ts in timestamps
        if ts.day == 1 and ts.hour == 0 and ts.minute == 0 and ts.second == 0
    ]

    if len(month_like) >= max(1, int(len(timestamps) * 0.8)):
        return "month"

    day_like = [
        ts for ts in timestamps
        if ts.hour == 0 and ts.minute == 0 and ts.second == 0
    ]

    if len(day_like) >= max(1, int(len(timestamps) * 0.8)):
        return "day"

    return "hour"


def _display_category_labels(values: list[Any]) -> list[str]:
    if not values:
        return []

    if any(_is_datetime_like(value) for value in values):
        granularity = _infer_time_granularity(values)
        return [
            _format_datetime_label(value, granularity=granularity)
            for value in values
        ]

    return [str(_scalar(value) or "") for value in values]

def _clean_chart_title(title: str | None, *, max_len: int = 90) -> str | None:
    if not title:
        return title

    text = str(title).strip()

    if len(text) <= max_len:
        return text

    return f"{text[:max_len - 1]}…"
