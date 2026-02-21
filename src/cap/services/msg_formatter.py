"""Formatter for converting cached queries into LLM message format."""
import json
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class MessageFormatter:
    """Convert cached query pairs into LLM prompt messages."""

    @staticmethod
    def format_similar_queries_to_examples(
        similar_queries: List[Dict[str, Any]],
        max_examples: int = 3
    ) -> List[Dict[str, str]]:
        """
        Format similar queries as few-shot examples.

        Args:
            similar_queries: List of similar cached queries
            max_examples: Maximum number of examples to include

        Returns:
            List of example pairs as dicts with 'user' and 'assistant' keys
        """
        examples = []

        examples_to_use = similar_queries[:max_examples]
        for example in examples_to_use:
            examples.append({
                "user": f"User Question: {example['original_query']}",
                "assistant": f"Assistant Answer: {example['sparql_query']}"
            })

        return examples

    @staticmethod
    def append_examples_to_prompt(
        examples: List[Dict[str, str]],
        existing_prompt: str,
        include_separator: bool = True
    ) -> str:
        """
        Append few-shot examples to an existing prompt string.

        Args:
            examples: List of example dicts from format_similar_queries_to_examples
            existing_prompt: The base prompt string
            include_separator: Whether to add visual separators between examples

        Returns:
            Complete prompt string with few-shot examples appended
        """
        if not examples:
            return existing_prompt

        prompt_parts = [existing_prompt.rstrip()]

        # Add section header for examples
        prompt_parts.append("\n\nHere are some examples of natural language questions and their corresponding SPARQL queries:\n")

        # Add each example
        for i, example in enumerate(examples, 1):
            if include_separator:
                prompt_parts.append(f"\n--- Example {i} ---")

            prompt_parts.append(f"\n{example['user']}")
            prompt_parts.append(f"\n{example['assistant']}")

            if include_separator and i < len(examples):
                prompt_parts.append("\n")

        return "".join(prompt_parts)
