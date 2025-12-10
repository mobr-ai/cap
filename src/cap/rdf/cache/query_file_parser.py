"""
Redis client for caching SPARQL queries and natural language mappings.
"""
import json
import logging
import re
from typing import Tuple
from opentelemetry import trace

from cap.util.sparql_util import ensure_validity

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

class QueryFileParser:
    """Parse query files with NL-SPARQL pairs."""

    @staticmethod
    def parse(content: str) -> list[Tuple[str, str]]:
        """Parse query file content into (natural_language, sparql) pairs."""
        queries = []
        lines = content.strip().split('\n')

        current_nl_query = None
        current_sparql_lines = []
        in_sparql = False
        in_triple_quotes = False

        for line in lines:
            if not line.strip() and not in_sparql:
                continue

            if line.strip().startswith('MESSAGE user'):
                if current_nl_query and current_sparql_lines:
                    sparql_query = '\n'.join(current_sparql_lines).strip()
                    sparql_query = QueryFileParser._extract_sparql(sparql_query, current_nl_query)
                    queries.append((current_nl_query, sparql_query))

                current_nl_query = line.strip().replace('MESSAGE user', '').strip()
                current_sparql_lines = []
                in_sparql = False
                in_triple_quotes = False

            elif line.strip().startswith('MESSAGE assistant'):
                in_sparql = True
                remaining = line.strip().replace('MESSAGE assistant', '').strip()

                if remaining == '"""':
                    in_triple_quotes = True
                elif remaining:
                    current_sparql_lines.append(remaining)

            elif in_sparql:
                stripped = line.strip()

                if stripped == '"""':
                    if in_triple_quotes:
                        in_triple_quotes = False
                        in_sparql = False
                    else:
                        in_triple_quotes = True
                    continue

                if in_triple_quotes or not stripped.startswith('MESSAGE'):
                    current_sparql_lines.append(line.rstrip())

        if current_nl_query and current_sparql_lines:
            sparql_query = '\n'.join(current_sparql_lines).strip()
            sparql_query = QueryFileParser._extract_sparql(sparql_query, current_nl_query)
            queries.append((current_nl_query, sparql_query))

        return queries

    @staticmethod
    def _extract_sparql(sparql: str, nl_query) -> str:
        """Clean and normalize SPARQL from file."""
        sparql = sparql.strip()

        if sparql.startswith('"""') and sparql.endswith('"""'):
            sparql = sparql[3:-3].strip()

        # Check if sequential
        if '---split' in sparql or '---query' in sparql:
            queries = []
            parts = re.split(r'---query\s+\d+[^-]*---', sparql)

            for part in parts[1:]:
                part = part.strip()
                if not part or part.startswith('---'):
                    continue

                part = re.sub(r'---split[^-]*---', '', part).strip()
                queries.append({
                    'query': part,
                    'inject_params': []
                })

            return json.dumps(queries)

        return ensure_validity(sparql, nl_query)
