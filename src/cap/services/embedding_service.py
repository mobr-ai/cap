"""
Manages sentence embeddings and vector search for cached NL queries.

Model  : intfloat/multilingual-e5-small (SentenceTransformer)
Storage: ChromaDB (local persistent, cosine similarity)

Single responsibility: encode text → store/query vectors.
Knows nothing about Redis, regeneration policy, or the NL pipeline.
"""
import asyncio
import logging
from typing import Any, Optional

import chromadb
from chromadb.config import Settings as ChromaSettings
from opentelemetry import trace
from sentence_transformers import SentenceTransformer

from cap.services.redis_nl_client import get_redis_nl_client

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

#_MODEL_NAME = "intfloat/multilingual-e5-small"
_MODEL_NAME = "all-MiniLM-L6-v2"
_COLLECTION_NAME = "nlq_cache"
_CHROMA_PATH = "./chroma_nlq_store"

# e5 models expect task prefixes for asymmetric search
_QUERY_PREFIX = "query: "
_PASSAGE_PREFIX = "passage: "


class EmbeddingService:
    """
    Encapsulates the embedding model and ChromaDB collection.

    rebuild() — indexes a fresh snapshot of all cached entries.
    search()  — returns top-N similar entries by cosine similarity.

    Both methods are async; CPU-bound encoding is dispatched to the
    default thread-pool executor so the event loop is never blocked.
    Concurrent rebuilds are serialised via an asyncio.Lock.
    """

    def __init__(
        self,
        model_name: str = _MODEL_NAME,
        chroma_path: str = _CHROMA_PATH,
        collection_name: str = _COLLECTION_NAME,
    ) -> None:
        self._model_name = model_name
        self._chroma_path = chroma_path
        self._collection_name = collection_name

        self._model: Optional[SentenceTransformer] = None
        self._chroma_client: Optional[chromadb.PersistentClient] = None
        self._collection = None
        self._rebuild_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Lazy initialisation — never called at import time
    # ------------------------------------------------------------------

    def _get_model(self) -> SentenceTransformer:
        if self._model is None:
            logger.info(f"Loading embedding model '{self._model_name}' …")
            self._model = SentenceTransformer(self._model_name, device="cpu")
            logger.info("Embedding model ready.")

        return self._model

    def _get_collection(self):
        if self._chroma_client is None:
            self._chroma_client = chromadb.PersistentClient(
                path=self._chroma_path,
                settings=ChromaSettings(anonymized_telemetry=False),
            )
        if self._collection is None:
            self._collection = self._chroma_client.get_or_create_collection(
                name=self._collection_name,
                metadata={"hnsw:space": "cosine"},
            )
        return self._collection

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def rebuild(self, cached_entries: list[dict[str, Any]]) -> int:
        """
        Atomically rebuild the ChromaDB collection from `cached_entries`.

        Each entry must contain at minimum:
            original_query (str), normalized_query (str),
            sparql_query (str), is_sequential (bool), precached (bool)

        Returns the number of documents indexed.
        """
        with tracer.start_as_current_span("embedding_service.rebuild") as span:
            async with self._rebuild_lock:
                if not cached_entries:
                    logger.warning("rebuild() called with no entries — skipping.")
                    span.set_attribute("indexed", 0)
                    return 0

                try:
                    loop = asyncio.get_event_loop()

                    original_queries = [e["original_query"] for e in cached_entries]
                    passages = [f"{_PASSAGE_PREFIX}{q}" for q in original_queries]

                    logger.info(f"Encoding {len(passages)} queries for embedding index …")

                    # Initialise the model on the event-loop thread before handing
                    # work to the executor. SentenceTransformer is not thread-safe
                    # during construction; only the stateless encode() call is safe
                    # to run in a worker thread.
                    model = self._get_model()
                    embeddings = await loop.run_in_executor(
                        None,
                        lambda: model.encode(
                            passages,
                            batch_size=64,
                            show_progress_bar=False,
                            normalize_embeddings=True,
                        ).tolist(),
                    )

                    # Wipe the collection and repopulate atomically
                    collection = self._get_collection()
                    self._chroma_client.delete_collection(self._collection_name)
                    self._collection = self._chroma_client.get_or_create_collection(
                        name=self._collection_name,
                        metadata={"hnsw:space": "cosine"},
                    )

                    self._collection.upsert(
                        ids=[f"nlq_{i}" for i in range(len(cached_entries))],
                        embeddings=embeddings,
                        documents=original_queries,
                        metadatas=[
                            {
                                "original_query": e["original_query"],
                                "normalized_query": e.get("normalized_query", ""),
                                "sparql_query": e.get("sparql_query", ""),
                                "is_sequential": str(e.get("is_sequential", False)),
                                "precached": str(e.get("precached", False)),
                            }
                            for e in cached_entries
                        ],
                    )

                    count = len(cached_entries)
                    logger.info(f"Embedding index rebuilt: {count} documents.")
                    span.set_attribute("indexed", count)
                    return count

                except Exception as exc:
                    span.set_attribute("error", str(exc))
                    logger.error(f"Embedding index rebuild failed: {exc}", exc_info=True)
                    raise

    async def search(
        self,
        nl_query: str,
        top_n: int = 5,
        min_similarity: float = 0.0,
    ) -> list[dict[str, Any]]:
        """
        Return the top-N most similar cached queries by cosine similarity.

        Result dicts contain:
            original_query, normalized_query, sparql_query,
            is_sequential (bool), precached (bool), similarity_score (float)
        """
        with tracer.start_as_current_span("embedding_service.search") as span:
            span.set_attribute("input_query", nl_query)

            try:
                collection = self._get_collection()

                if collection.count() == 0:
                    logger.debug("ChromaDB collection is empty — returning no results.")
                    return []

                loop = asyncio.get_event_loop()
                model = self._get_model()
                query_embedding = await loop.run_in_executor(
                    None,
                    lambda: model.encode(
                        [f"{_QUERY_PREFIX}{nl_query}"],
                        normalize_embeddings=True,
                    ).tolist(),
                )

                raw = collection.query(
                    query_embeddings=query_embedding,
                    n_results=min(top_n, collection.count()),
                    include=["metadatas", "distances"],
                )

                results: list[dict[str, Any]] = []
                for meta, distance in zip(
                    raw.get("metadatas", [[]])[0],
                    raw.get("distances", [[]])[0],
                ):
                    similarity = float(1.0 - distance)
                    if similarity < min_similarity:
                        continue

                    cached_normalized = meta.get("normalized_query", "")
                    original_nl = meta.get("original_query", "")
                    redis_client = get_redis_nl_client()
                    sparql_data = await redis_client.get_cached_query_with_original(
                        normalized_query=cached_normalized,
                        original_query=original_nl,
                    )
                    results.append({
                        "original_query": meta.get("original_query", ""),
                        "normalized_query": meta.get("normalized_query", ""),
                        "sparql_query": sparql_data["sparql_query"],
                        "is_sequential": meta.get("is_sequential", "False") == "True",
                        "precached": meta.get("precached", "False") == "True",
                        "similarity_score": similarity,
                    })

                results.sort(key=lambda x: x["similarity_score"], reverse=True)
                span.set_attribute("results_found", len(results))
                return results

            except Exception as exc:
                span.set_attribute("error", str(exc))
                logger.error(f"Embedding search failed: {exc}", exc_info=True)
                raise


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
_embedding_service: Optional[EmbeddingService] = None


def get_embedding_service() -> EmbeddingService:
    global _embedding_service
    if _embedding_service is None:
        _embedding_service = EmbeddingService()
    return _embedding_service