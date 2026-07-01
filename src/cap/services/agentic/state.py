from typing import Any, TypedDict

from cap.federated.models import FederatedExecutionResult, FederatedQuery


class AgenticQueryState(TypedDict, total=False):
    user_query: str
    context: str | None
    conversation_history: list[dict[str, Any]] | None
    normalized_query: str
    cached: bool
    query_valid: bool
    federated_query: FederatedQuery | None
    execution_result: FederatedExecutionResult | None
    formatted_results: str
    kv_results: Any
    final_answer: str
    error: str | None
    retry_count: int
    max_retries: int
    refer_decision: Any
    infrastructure_limit_exceeded: bool
    post_processed: bool
