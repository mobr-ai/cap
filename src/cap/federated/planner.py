import json
import logging
from typing import Any

from cap.federated.models import FederatedQuery, PostProcessingConfig, QuerySource
from cap.federated.offchain_schema import OFFCHAIN_SCHEMA
from cap.federated.sparql.sparql_util import ensure_validity
from cap.federated.sql.sql_util import clean_sql
from cap.services.prompt_builder import PromptBuilder

logger = logging.getLogger(__name__)

class FederatedPlanner:
    def __init__(self, llm_client: Any):
        self.llm_client = llm_client
        self.prompt_builder = PromptBuilder()

    async def generate(
        self,
        natural_query: str,
        conversation_history: list[dict] | None,
        ontology_block: str,
        fewshot_block: str = "",
    ) -> FederatedQuery:

        prompt = f"""
{self.prompt_builder.federated_prompt}

Ontology:
{ontology_block}

Offchain data relational data model:
{OFFCHAIN_SCHEMA}

Relevant cached query patterns:
{fewshot_block}

User Question:
{natural_query}
""".strip()

        chunks: list[str] = []
        async for chunk in self.llm_client.generate_stream(
            prompt=prompt,
            model=self.llm_client.llm_model,
            system_prompt="",
            temperature=0.0,
        ):
            chunks.append(chunk)

        raw = "".join(chunks).strip()
        parsed = self._parse_json(raw)

        visualization_type = parsed.get("visualization_type", "") or ""
        sparql = parsed.get("sparql", "") or ""
        sql = clean_sql(parsed.get("sql", "") or "")
        source = parsed.get("source") or self._infer_source(sparql, sql)
        explanation=parsed.get("explanation", "") or ""
        language = parsed.get("language", "en") or "en"

        post_processing_raw = parsed.get("post_processing")
        post_processing = None
        if isinstance(post_processing_raw, dict):
            post_processing = PostProcessingConfig.model_validate(post_processing_raw)

        if sparql:
            sparql = ensure_validity(sparql, natural_query)

        return FederatedQuery(
            visualization_type=visualization_type,
            sparql=sparql,
            sql=sql,
            source=QuerySource(source),
            explanation=explanation,
            language=language,
            post_processing=post_processing,
            nl_query=natural_query,
        )

    @staticmethod
    def _parse_json(raw: str) -> dict[str, Any]:
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            start = raw.find("{")
            end = raw.rfind("}")
            if start >= 0 and end > start:
                return json.loads(raw[start:end + 1])
            raise ValueError(
                f"LLM did not return valid federated JSON: {raw[:500]}"
            ) from e

    @staticmethod
    def _infer_source(sparql: str, sql: str) -> str:
        if sparql and sql:
            return QuerySource.FEDERATED.value
        if sql:
            return QuerySource.OFFCHAIN.value
        return QuerySource.ONCHAIN.value
