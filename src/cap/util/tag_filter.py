from dataclasses import dataclass

@dataclass
class TagFilter:
    """
    Stateful streaming filter that removes <think>...</think> blocks,
    even when tags arrive split across streamed chunks.
    """
    in_think: bool = False
    carry: str = ""  # holds partial tag fragments across chunks

    OPEN = "<think>"
    CLOSE = "</think>"

    def reset(self) -> None:
        self.in_think = False
        self.carry = ""

    def push(self, chunk: str) -> str:
        """
        Feed a new streamed text chunk. Returns text safe to display.
        """
        if not chunk:
            return ""

        s = self.carry + chunk
        self.carry = ""

        out_parts: list[str] = []
        i = 0

        while i < len(s):
            if self.in_think:
                # We are inside <think> ... </think>, discard until CLOSE.
                close_idx = s.find(self.CLOSE, i)
                if close_idx == -1:
                    # No close tag in the current buffer.
                    # Keep only a small suffix in case CLOSE is split across chunks.
                    keep = len(self.CLOSE) - 1
                    if len(s) - i > keep:
                        self.carry = s[-keep:]
                    else:
                        self.carry = s[i:]
                    return "".join(out_parts)

                # Found closing tag, jump past it and stop discarding.
                i = close_idx + len(self.CLOSE)
                self.in_think = False
                continue

            # Not in think: look for OPEN tag.
            open_idx = s.find(self.OPEN, i)
            if open_idx == -1:
                # No open tag: emit rest, but keep possible partial "<think" fragment.
                # We keep up to len(OPEN)-1 trailing chars to catch split OPEN tag.
                keep = len(self.OPEN) - 1
                if len(s) - i > keep:
                    emit_upto = len(s) - keep
                    out_parts.append(s[i:emit_upto])
                    self.carry = s[emit_upto:]
                else:
                    self.carry = s[i:]
                return "".join(out_parts)

            # Emit everything before OPEN tag.
            out_parts.append(s[i:open_idx])

            # Enter think mode and skip the OPEN tag itself.
            i = open_idx + len(self.OPEN)
            self.in_think = True

        return "".join(out_parts)

    def flush(self) -> str:
        """
        Called at end of stream. If we were outside think, return any leftover carry.
        If we were inside think, discard carry.
        """
        if self.in_think:
            self.carry = ""
            return ""
        leftover = self.carry
        self.carry = ""
        return leftover
