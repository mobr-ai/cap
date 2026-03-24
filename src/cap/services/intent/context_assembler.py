from typing import Any

from cap.services.embedding_service import get_embedding_service
from cap.services.intent.models import ReferDecision


class ConversationContextAssembler:
    def __init__(self, similarity_threshold: float = 0.55, max_items: int = 4) -> None:
        self._similarity_threshold = similarity_threshold
        self._max_items = max_items

    async def assemble(
        self,
        current_query: str,
        conversation_history: list[dict[str, Any]] | None,
        refer_decision: ReferDecision,
    ) -> list[dict[str, Any]]:
        if refer_decision.label != "refer" or not conversation_history:
            return []

        candidates = [
            item for item in conversation_history
            if item.get("content") and item.get("role") in {"user", "assistant"}
        ]
        if not candidates:
            return []

        embedder = get_embedding_service()
        query_vector = await embedder.encode_texts([current_query], prefix="query: ")
        history_vectors = await embedder.encode_texts(
            [item["content"] for item in candidates],
            prefix="passage: ",
        )

        scored: list[tuple[float, dict[str, Any]]] = []
        qv = query_vector[0]

        for item, hv in zip(candidates, history_vectors):
            score = embedder.cosine_similarity(qv, hv)
            if score >= self._similarity_threshold:
                scored.append((score, item))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [item for _, item in scored[: self._max_items]]