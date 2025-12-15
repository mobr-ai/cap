import logging
from dataclasses import dataclass

from opentelemetry import trace

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@dataclass
class PlaceholderCounters:
    """Track placeholder counters for SPARQL normalization."""
    pct: int = 0
    num: int = 0
    str: int = 0
    lim: int = 0
    uri: int = 0
    cur: int = 0
    inject: int = 0
    year: int = 0
    month: int = 0
    day: int = 0
    period: int = 0
    order: int = 0
    duration: int = 0
    definition: int = 0
    quantifier: int = 0

    def update_from_placeholder(self, placeholder) -> None:
        """Update counter based on placeholder type."""
        try:
            if placeholder.startswith("<<INJECT_"):
                idx = int(placeholder.replace('<<INJECT_', '').replace('>>', ''))
                self.inject = max(self.inject, idx + 1)
            elif placeholder.startswith("<<PCT_DECIMAL_"):
                idx = int(placeholder.replace('<<PCT_DECIMAL_', '').replace('>>', ''))
                self.pct = max(self.pct, idx + 1)
            elif placeholder.startswith("<<PCT_"):
                idx = int(placeholder.replace('<<PCT_', '').replace('>>', ''))
                self.pct = max(self.pct, idx + 1)
            elif placeholder.startswith("<<NUM_"):
                idx = int(placeholder.replace('<<NUM_', '').replace('>>', ''))
                self.num = max(self.num, idx + 1)
            elif placeholder.startswith("<<STR_"):
                idx = int(placeholder.replace('<<STR_', '').replace('>>', ''))
                self.str = max(self.str, idx + 1)
            elif placeholder.startswith("<<LIM_"):
                idx = int(placeholder.replace('<<LIM_', '').replace('>>', ''))
                self.lim = max(self.lim, idx + 1)
            elif placeholder.startswith("<<URI_"):
                idx = int(placeholder.replace('<<URI_', '').replace('>>', ''))
                self.uri = max(self.uri, idx + 1)
            elif placeholder.startswith("<<CUR_"):
                idx = int(placeholder.replace('<<CUR_', '').replace('>>', ''))
                self.cur = max(self.cur, idx + 1)
            elif placeholder.startswith("<<DURATION_"):
                idx = int(placeholder.replace('<<DURATION_', '').replace('>>', ''))
                self.duration = max(self.duration, idx + 1)
            elif placeholder.startswith("<<DEF_"):
                idx = int(placeholder.replace('<<DEF_', '').replace('>>', ''))
                self.definition = max(self.definition, idx + 1)
            elif placeholder.startswith("<<QUANT_"):
                idx = int(placeholder.replace('<<QUANT_', '').replace('>>', ''))
                self.quantifier = max(self.quantifier, idx + 1)
        except (AttributeError, ValueError) as e:
            logger.warning(f"Failed to parse index from {placeholder}: {e}")
