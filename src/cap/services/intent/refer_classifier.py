import logging
from pathlib import Path

from cap.services.embedding_service import get_embedding_service
from cap.services.intent.example_loader import ExampleLoader
from cap.services.intent.models import ReferDecision

logger = logging.getLogger(__name__)

class ReferClassifier:
    def __init__(
        self,
        dataset_path: str,
        collection_name: str = "refer_classifier_examples",
        min_confidence: float = 0.58,
    ) -> None:

        path = Path(dataset_path)
        if not path.exists():
            logger.warning(f"-- dataset file not found: {dataset_path}")

        self._dataset_path = path
        self._collection_name = collection_name
        self._min_confidence = min_confidence

    async def warmup(self) -> None:
        rows = ExampleLoader.load_jsonl(self._dataset_path)
        await get_embedding_service().rebuild_dataset_collection(
            collection_name=self._collection_name,
            rows=rows,
            text_key="text",
        )

    async def classify(self, query: str) -> ReferDecision:
        results = await get_embedding_service().search_dataset_collection(
            collection_name=self._collection_name,
            query_text=query,
            top_n=3,
        )
        if not results:
            return ReferDecision(label="not_refer", confidence=0.0, reason="no_match")

        top = results[0]
        label = top["metadata"]["label"]
        confidence = float(top["similarity_score"])

        if confidence < self._min_confidence:
            return ReferDecision(label="not_refer", confidence=confidence, reason="below_threshold")

        return ReferDecision(label=label, confidence=confidence, reason="semantic_match")