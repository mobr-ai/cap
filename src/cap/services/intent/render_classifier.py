import logging
from pathlib import Path

from cap.services.embedding_service import get_embedding_service
from cap.services.intent.example_loader import ExampleLoader
from cap.services.intent.models import RenderDecision

logger = logging.getLogger(__name__)

class RenderClassifier:
    def __init__(
        self,
        dataset_path: str,
        collection_name: str = "render_classifier_examples",
        min_family_confidence: float = 0.56,
        min_chart_confidence: float = 0.56,
    ) -> None:

        path = Path(dataset_path)
        if not path.exists():
            logger.warning(f"-- dataset file not found: {dataset_path}")

        self._dataset_path = path
        self._collection_name = collection_name
        self._min_family_confidence = min_family_confidence
        self._min_chart_confidence = min_chart_confidence

    async def warmup(self) -> None:
        rows = ExampleLoader.load_jsonl(self._dataset_path)
        await get_embedding_service().rebuild_dataset_collection(
            collection_name=self._collection_name,
            rows=rows,
            text_key="text",
        )

    async def classify(self, query: str) -> RenderDecision:
        results = await get_embedding_service().search_dataset_collection(
            collection_name=self._collection_name,
            query_text=query,
            top_n=5,
        )
        if not results:
            return RenderDecision(family=None, confidence=0.0)

        family_votes: dict[str, float] = {}
        best_chart_subtype: tuple[str, float] | None = None
        matched_example: str | None = None

        for item in results:
            metadata = item["metadata"]
            score = float(item["similarity_score"])
            family = metadata.get("family")
            subtype = metadata.get("chart_subtype")
            example_text = metadata.get("text")

            if family:
                family_votes[family] = family_votes.get(family, 0.0) + score

            if family == "chart" and subtype:
                if best_chart_subtype is None or score > best_chart_subtype[1]:
                    best_chart_subtype = (subtype, score)
                    matched_example = example_text

        if not family_votes:
            return RenderDecision(family=None, confidence=0.0)

        family, family_score = max(family_votes.items(), key=lambda x: x[1])

        if family_score < self._min_family_confidence:
            return RenderDecision(family=None, confidence=family_score)

        if family != "chart":
            return RenderDecision(
                family=family,
                confidence=family_score,
                matched_example=matched_example,
            )

        subtype = best_chart_subtype[0] if best_chart_subtype else None
        subtype_confidence = best_chart_subtype[1] if best_chart_subtype else 0.0

        if subtype is None or subtype_confidence < self._min_chart_confidence:
            return RenderDecision(family="chart", confidence=family_score, chart_subtype="line")

        return RenderDecision(
            family="chart",
            confidence=family_score,
            chart_subtype=subtype,
            matched_example=matched_example,
        )