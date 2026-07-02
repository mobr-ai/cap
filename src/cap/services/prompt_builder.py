"""
Prompt builder for LLM workflows.
Keeps prompt loading, ontology injection, few-shot examples, and history handling
outside the transport-focused LLM client.
"""

import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

from cap.chains.registry import get_chain
from cap.config import settings
from cap.services.msg_formatter import MessageFormatter
from cap.services.similarity_service import SearchStrategy, SimilarityService
from cap.services.vega.facade import VegaConverter
from cap.util.str_util import get_file_content

logger = logging.getLogger(__name__)

MAX_CONTEXT_CHARS = 18000

INFRA_ISSUE = "infra_issue"

def is_infrastructure_limit_error(error_msg: str | None) -> bool:
    if not error_msg:
        return False

    normalized_err = error_msg.lower()

    return (
        "infrastructure_limit_exceeded" in normalized_err
        or "429" in normalized_err
        or "500" in normalized_err
        or "network error" in normalized_err
        or "too many requests" in normalized_err
        or "operation timed out" in normalized_err
    )

class PromptBuilder:
    """Builds prompts and prompt fragments used by LLM workflows."""

    def __init__(self, fewshot_top_n: int | None = None):
        self.fewshot_top_n = fewshot_top_n or int(os.getenv("FEWSHOT_TOP_N") or "3")

    def _load_prompt(self, env_key: str, default: str = "") -> str:
        return os.getenv(env_key, default)

    @property
    def default_nl_to_sparql_prompt(self) -> str:
        return self._load_prompt(
            "NL_TO_SPARQL_PROMPT",
            get_chain().default_nl_to_sparql_prompt(),
        )


    @property
    def default_chart_prompt(self) -> str:
        return self._load_prompt(
            "CHART_PROMPT",
            get_chain().default_chart_prompt(),
        )

    @property
    def ontology_prompt(self) -> str:
        if settings.LLM_ONTOLOGY_PATH != "":
            onto = get_file_content(settings.LLM_ONTOLOGY_PATH)
            return f"ALWAYS USE THIS ONTOLOGY:\n{onto}"

        logger.warning("** MINI ONTOLOGY NOT FOUND!!!")
        return ""

    @property
    def infra_limit_exceeded_prompt(self) -> str:
        return self._load_prompt(
            "INFRA_LIMIT_EXCEEDED_PROMPT",
            "Write a short, clear answer to the user. "
            "Explain that the query was too demanding for the current infrastructure, "
            "so it could not be completed right now. If the user wants to support the project "
            "to improve the infrastructure, donations and subscriptions are welcomed visiting https://cap.mobr.ai/settings"
            "Do not expose stack traces, HTTP details, internal service names, or raw errors. "
            "Be polite and concise.\n\n",
        )

    @property
    def no_data_prompt(self) -> str:
        return self._load_prompt(
            "NO_DATA_PROMPT",
            "Answer with a variation of to the following message:"
            "I do not have this information or I was not capable of retrieving it correctly."
            "We would appreciate it if you could specify here what you wanted to do as a feature "
            "and we will try to make your prompt work asap. If you think this feature is already "
            "supported, try specifying the entire command in a unique precise prompt.\n\n"
            "In addition, ask if the user wants to support the project so this issue can be "
            "addressed faster, by asking politely for donation and subscriptions, by "
            "visiting https://cap.mobr.ai/settings",
        )

    @property
    def federated_prompt(self) -> str:
        return self._load_prompt(
            "FEDERATED_PROMPT",
            "Create a federated query in a json, specifying sql for asset price movement and sparql for onchain data.",
        )

    @property
    def contextualize_prompt(self) -> str:
        return self._load_prompt(
            "CONTEXTUALIZE_PROMPT",
            "Based on the query results, provide a clear and helpful answer.",
        )


    def get_syncing_prompt(self, remaining_time) -> str:
        return self._load_prompt(
            "SYNCING_PROMPT",
            f"The platform periodically reindexes its knowledge graph every four days to ensure optimal query performance and data consistency. Reindexing is currently in progress and is expected to complete in approximately {remaining_time}. Thank you for your patience.",
        )


    async def build_fewshot_block(
        self,
        nl_query: str,
        strategy: SearchStrategy = SearchStrategy.auto,
        top_n: int = -1,
        min_similarity: float = 0.0,
        _eval_retrieved_out: list[dict[str, Any]] | None = None,
    ) -> str:
        effective_top_n = top_n if top_n != -1 else self.fewshot_top_n

        similar = await SimilarityService.find_similar_queries(
            strategy=strategy,
            nl_query=nl_query,
            top_n=effective_top_n,
            min_similarity=min_similarity,
        )

        if _eval_retrieved_out is not None:
            _eval_retrieved_out.extend(similar)

        messages = MessageFormatter.format_similar_queries_to_examples(
            similar_queries=similar,
            max_examples=effective_top_n,
        )

        return MessageFormatter.append_examples_to_prompt(
            examples=messages,
            existing_prompt="",
        )


    def add_history(
        self,
        prompt: str,
        conversation_history: list[dict[str, Any]] | None = None,
    ) -> str:
        history: list[dict[str, Any]] = []

        if conversation_history:
            reversed_history = list(reversed(conversation_history))
            kept_history: list[dict[str, Any]] = []
            current_size = len(prompt)

            for msg in reversed_history:
                msg_size = len(msg.get("content", ""))
                if current_size + msg_size < MAX_CONTEXT_CHARS:
                    kept_history.append(msg)
                    current_size += msg_size
                else:
                    logger.info(
                        "Truncated conversation history at %s messages due to context limit",
                        len(kept_history),
                    )
                    break

            history = list(reversed(kept_history))

        str_history = "\n".join(
            f"{msg.get('role', 'unknown')}: {msg.get('content', '')}"
            for msg in history
        )

        return f"{prompt}\nPrevious messages:\n{str_history}" if str_history else prompt


    def build_answer_prompt(
        self,
        user_query: str,
        formatted_results: str | dict,
        result_type: str,
        kv_results: dict[str, Any],
        conversation_history: list[dict[str, Any]] | None,
    ) -> tuple[str, list[dict[str, Any]] | None, float]:
        current_date = f"Current utc date and time: {datetime.now(UTC)}."
        current_his = None
        temperature = 0.1

        is_infra_issue = False
        context_res = ""
        try:
            # If results are already formatted as string, use directly
            if isinstance(formatted_results, str):
                if formatted_results == INFRA_ISSUE:
                    is_infra_issue = True
                else:
                    context_res = formatted_results

            # Otherwise, serialize dict to JSON
            elif formatted_results:
                context_res = json.dumps(formatted_results, indent=2)
            else:
                context_res = ""

        except Exception as e:
            logger.warning(f"Result formatting failed: {e}")
            context_res = str(formatted_results)

        if result_type in VegaConverter.known_types:
            known_info = f"""
                {current_date}
                {self.default_chart_prompt}
                The system is showing an artifact to the user using the data below. Always write a SHORT insight about it.
                {kv_results}
            """

        elif context_res != "":
            known_info = f"""
                {current_date}
                This is the current value you MUST consider in your answer:
                {context_res}

                {self.contextualize_prompt}
            """
            current_his = conversation_history

        else:
            if is_infra_issue:
                known_info = self.infra_limit_exceeded_prompt
            else:
                known_info = self.no_data_prompt

        prompt = f"""
            User Question: {user_query}

            {known_info}
        """

        return self.add_history(prompt, current_his), temperature
