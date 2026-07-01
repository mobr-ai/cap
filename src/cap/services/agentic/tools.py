import json
from typing import Any

from langchain_core.tools import tool

from cap.chains.cardano.canon.query_normalizer import QueryNormalizer
from cap.federated.federated_result_processor import merge_federated_kv_results
from cap.federated.models import FederatedQuery, PostProcessingConfig, QuerySource
from cap.federated.service import execute_federated_query
from cap.federated.sparql.sparql_result_processor import convert_sparql_to_kv
from cap.federated.sql.sql_result_processor import normalize_sql_results
from cap.services.redis_nl_client import RedisNLClient
from cap.services.similarity_service import SimilarityService


@tool
async def normalize_query_tool(user_query: str) -> str:
    """Normalize a natural-language query for cache lookup."""
    return QueryNormalizer.normalize(user_query)


async def get_cached_federated_query(
    redis_client: RedisNLClient,
    normalized_query: str,
    user_query: str,
    normalize: bool = True,
) -> FederatedQuery | None:
    cached_data = await redis_client.get_cached_query_with_original(
        normalized_query=normalized_query,
        original_query=user_query,
        normalize=normalize,
    )
    if not cached_data:
        return None

    payload = cached_data["federated_query"]

    try:
        parsed = json.loads(payload)
        if isinstance(parsed, dict):
            visualization_type = parsed.get("visualization_type", "") or ""
            sparql = parsed.get("sparql", "") or ""
            sql = parsed.get("sql", "") or ""
            source = parsed.get("source") or _infer_source(sparql, sql).value
            explanation=parsed.get("explanation", "cached federated query") or ""
            language = parsed.get("language", "en") or "en"

            post_processing_raw = parsed.get("post_processing")
            post_processing = None
            if isinstance(post_processing_raw, dict):
                post_processing = PostProcessingConfig.model_validate(post_processing_raw)

            return FederatedQuery(
                visualization_type=visualization_type,
                sparql=sparql,
                sql=sql,
                source=QuerySource(source),
                explanation=explanation,
                language=language,
                post_processing=post_processing,
                nl_query=user_query,
            )

    except json.JSONDecodeError:
        pass

    return FederatedQuery(
        visualization_type="",
        sparql=payload,
        sql="",
        source=QuerySource.ONCHAIN,
        explanation="legacy SPARQL cache entry",
        language="en",
        post_processing=None,
        nl_query=user_query
    )


async def cache_successful_query(
    redis_client: RedisNLClient,
    user_query: str,
    federated_query: FederatedQuery,
    normalize: bool = True,
) -> None:

    payload = json.dumps(
        {
            "source": federated_query.source.value,
            "visualization_type": federated_query.visualization_type or "",
            "sparql": federated_query.sparql or "",
            "sql": federated_query.sql or "",
            "explanation": federated_query.explanation or "",
            "language": federated_query.language or "en",
            "post_processing": (
                federated_query.post_processing.model_dump()
                if federated_query.post_processing
                else None
            ),
        },
        sort_keys=True,
    )

    result = await redis_client.cache_query(
        nl_query=user_query,
        payload=payload,
        normalize=normalize,
    )

    if result == 1:
        await SimilarityService.notify_new_cache_entry()


async def execute_query_tool(query: FederatedQuery):
    return await execute_federated_query(query)


def format_execution_context(
    federated_query: FederatedQuery,
    sparql_results: dict[str, Any],
    sql_results: list[dict[str, Any]],
) -> tuple[str, Any]:
    sections: list[str] = []

    sparql_kv: dict[str, Any] | None = None
    sql_kv: dict[str, Any] | None = None

    if federated_query.sparql:
        sparql_kv = convert_sparql_to_kv(
            sparql_results,
            federated_query.sparql,
        )
        sections.append(
            "SPARQL results:\n"
            + json.dumps(sparql_kv, default=str, ensure_ascii=False, indent=2)
        )

    if federated_query.sql:
        normalized_sql_results = normalize_sql_results(sql_results)

        sql_kv = {
            "result_type": "multiple" if len(normalized_sql_results) > 1 else "single",
            "count": len(normalized_sql_results),
            "data": normalized_sql_results,
        }

        sections.append(
            "SQL results:\n"
            + json.dumps(sql_kv, default=str, ensure_ascii=False, indent=2)
        )

    if sparql_kv and sql_kv:
        kv_results = merge_federated_kv_results(sparql_kv, sql_kv)
    else:
        kv_results = sparql_kv or sql_kv

    kv_results["title"] = federated_query.nl_query[:120]

    return "\n\n".join(sections), kv_results


def _infer_source(sparql: str, sql: str) -> QuerySource:
    if sparql and sql:
        return QuerySource.FEDERATED
    if sql:
        return QuerySource.OFFCHAIN
    return QuerySource.ONCHAIN
