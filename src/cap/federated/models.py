from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field


class QuerySource(StrEnum):
    ONCHAIN = "onchain"
    OFFCHAIN = "offchain"
    FEDERATED = "federated"


class PostProcessingConfig(BaseModel):
    type: Literal[
        "cumulative_sum",
        "cumulative_count",
        "ratio",
        "percentage",
        "derived_field",
    ]
    sort_by: str | None = None
    source_fields: list[str] = Field(default_factory=list)
    target_field: str


class FederatedQuery(BaseModel):
    visualization_type: str = ""
    sparql: str = ""
    sql: str = ""
    source: QuerySource
    explanation: str = ""
    language: str = "en"
    post_processing: PostProcessingConfig | None = None
    nl_query: str = ""


class FederatedExecutionResult(BaseModel):
    has_data: bool
    sparql_results: dict[str, Any] = Field(default_factory=dict)
    sql_results: list[dict[str, Any]] = Field(default_factory=list)
    error_msg: str = ""
