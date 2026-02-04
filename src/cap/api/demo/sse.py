# cap/api/demo/sse.py
import re
from typing import Iterator

from cap.api.demo.schemas import NL_TOKEN

# Used by router break_sse regression modes (concat_payload / concat_raw)
DONE_SSE = "data: [DONE]"


def _iter_safe_line_chunks(line: str, max_len: int = 96) -> Iterator[str]:
    """
    Chunk a single line (NO '\n' inside) into <= max_len pieces.
    Avoid emitting chunks ending in space/tab (frontend trims them).
    """
    if line is None:
        return
    s = str(line)
    if s == "":
        return
    max_len = max(16, int(max_len))

    i = 0
    carry = ""
    while i < len(s):
        take = s[i : i + max_len]
        i += max_len

        take = carry + take
        carry = ""

        m = re.search(r"[ \t]+$", take)
        if m:
            ws = m.group(0)
            take = take[: -len(ws)]
            carry = ws + carry

        if take:
            yield take


def iter_sse_markdown_events(text: str, max_len: int = 32) -> Iterator[str]:
    """
    Produce a stream of 'data:' payload strings.
    We emit NL_TOKEN as its own payload for every newline in the original text.
    """
    if not text:
        return
    raw = str(text).replace("\r\n", "\n").replace("\r", "\n")
    lines = raw.split("\n")

    for idx, line in enumerate(lines):
        for chunk in _iter_safe_line_chunks(line, max_len=max_len):
            yield chunk
        if idx < len(lines) - 1:
            yield NL_TOKEN
