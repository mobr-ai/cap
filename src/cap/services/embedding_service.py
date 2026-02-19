"""
Manages sentence embeddings for cached NL queries using:
  - SentenceTransformer  (multilingual-e5-small)
  - ChromaDB             (local persistent vector store)

This service is the single owner of both the embedding model and the
vector collection. It exposes only two public async methods:
  - rebuild(all_cached_entries)  →  indexes every cached original query
  - search(query, top_n)         →  returns top-N similar entries
"""
import asyncio
import logging
from typing import Any, Optional

import chromadb
from chromadb.config import Settings as ChromaSettings
from opentelemetry import trace
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

_MODEL_NAME = "intfloat/multilingual-e5-small"
_COLLECTION_NAME = "nlq_cache"
_CHROMA_PATH = "./chroma_nlq_store"

# e5 models expect a task prefix for queries and passages
_QUERY_PREFIX = "query: "
_PASSAGE_PREFIX = "passage: "


class EmbeddingService:
    """
    Singleton-friendly service that keeps the embedding model and ChromaDB
    collection in memory. Thread-safe for reads; rebuild is serialised via
    an asyncio Lock.
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
    # Initialisation helpers
    # ------------------------------------------------------------------

    def _ensure_model(self) -> SentenceTransformer:
        if self._model is None:
            logger.info(f"Loading embedding model '{self._model_name}' …")
            self._model = SentenceTransformer(self._model_name)
            logger.info("Embedding model loaded.")
        return self._model

    def _ensure_collection(self):
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
        Rebuild the ChromaDB collection from scratch using every entry in
        `cached_entries`.

        Each entry is expected to have at minimum:
            {
                "original_query": str,
                "normalized_query": str,
                "sparql_query": str,
                "is_sequential": bool,
                "precached": bool,
            }

        Returns the number of documents indexed.
        """
        with tracer.start_as_current_span("embedding_service.rebuild") as span:
            async with self._rebuild_lock:
                try:
                    model = await asyncio.get_event_loop().run_in_executor(
                        None, self._ensure_model
                    )
                    collection = self._ensure_collection()

                    if not cached_entries:
                        logger.warning("rebuild() called with empty cached_entries; skipping.")
                        span.set_attribute("indexed", 0)
                        return 0

                    original_queries = [e["original_query"] for e in cached_entries]
                    passages = [f"{_PASSAGE_PREFIX}{q}" for q in original_queries]

                    logger.info(
                        f"Encoding {len(passages)} cached queries for embedding index …"
                    )
                    embeddings = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: model.encode(
                            passages,
                            batch_size=64,
                            show_progress_bar=False,
                            normalize_embeddings=True,
                        ).tolist(),
                    )

                    # Wipe and repopulate atomically
                    self._chroma_client.delete_collection(self._collection_name)
                    self._collection = self._chroma_client.get_or_create_collection(
                        name=self._collection_name,
                        metadata={"hnsw:space": "cosine"},
                    )

                    ids = [f"nlq_{i}" for i in range(len(cached_entries))]
                    metadatas = [
                        {
                            "original_query": e["original_query"],
                            "normalized_query": e.get("normalized_query", ""),
                            "sparql_query": e.get("sparql_query", ""),
                            "is_sequential": str(e.get("is_sequential", False)),
                            "precached": str(e.get("precached", False)),
                        }
                        for e in cached_entries
                    ]

                    # ChromaDB upsert in a single call
                    self._collection.upsert(
                        ids=ids,
                        embeddings=embeddings,
                        documents=original_queries,
                        metadatas=metadatas,
                    )

                    count = len(cached_entries)
                    logger.info(f"Embedding index rebuilt with {count} documents.")
                    span.set_attribute("indexed", count)
                    return count

                except Exception as exc:
                    span.set_attribute("error", str(exc))
                    logger.error(f"Failed to rebuild embedding index: {exc}", exc_info=True)
                    raise

    async def search(
        self,
        nl_query: str,
        top_n: int = 5,
        min_similarity: float = 0.0,
    ) -> list[dict[str, Any]]:
        """
        Find the top-N most similar cached queries via cosine similarity.

        Returns list of dicts:
            {
                "original_query": str,
                "normalized_query": str,
                "sparql_query": str,
                "is_sequential": bool,
                "precached": bool,
                "similarity_score": float,
            }
        """
        with tracer.start_as_current_span("embedding_service.search") as span:
            span.set_attribute("input_query", nl_query)
            span.set_attribute("top_n", top_n)

            try:
                collection = self._ensure_collection()

                if collection.count() == 0:
                    logger.debug("ChromaDB collection is empty; skipping embedding search.")
                    return []

                model = await asyncio.get_event_loop().run_in_executor(
                    None, self._ensure_model
                )

                query_embedding = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: model.encode(
                        [f"{_QUERY_PREFIX}{nl_query}"],
                        normalize_embeddings=True,
                    ).tolist(),
                )

                results = collection.query(
                    query_embeddings=query_embedding,
                    n_results=min(top_n, collection.count()),
                    include=["metadatas", "distances"],
                )

                hits = []
                metadatas = results.get("metadatas", [[]])[0]
                distances = results.get("distances", [[]])[0]

                for meta, distance in zip(metadatas, distances):
                    # ChromaDB cosine distance → similarity: 1 - distance
                    similarity = float(1.0 - distance)
                    if similarity < min_similarity:
                        continue
                    hits.append(
                        {
                            "original_query": meta.get("original_query", ""),
                            "normalized_query": meta.get("normalized_query", ""),
                            "sparql_query": meta.get("sparql_query", ""),
                            "is_sequential": meta.get("is_sequential", "False") == "True",
                            "precached": meta.get("precached", "False") == "True",
                            "similarity_score": similarity,
                        }
                    )

                hits.sort(key=lambda x: x["similarity_score"], reverse=True)
                span.set_attribute("results_found", len(hits))
                return hits

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