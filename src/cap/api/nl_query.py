"""
Natural language query API endpoint using Ollama LLM.
Multi-stage pipeline: NL -> SPARQL -> Execute -> Contextualize -> Stream
"""
import logging
from typing import Optional
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from opentelemetry import trace

from cap.services.ollama_client import get_ollama_client
from cap.data.virtuoso import VirtuosoClient

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

router = APIRouter(prefix="/api/v1/nl", tags=["llm"])


class NLQueryRequest(BaseModel):
    """Natural language query request."""
    query: str = Field(..., description="Natural language query", min_length=1, max_length=1000)
    context: Optional[str] = Field(None, description="Additional context for the query")

class StatusMessage:
    """Helper for creating consistent status messages."""

    @staticmethod
    def processing_query() -> str:
        return "status: Processing your query\n\n"

    @staticmethod
    def generating_sparql() -> str:
        return "status: Analyzing contexts in the knowledge graph\n\n"

    @staticmethod
    def executing_query() -> str:
        return "status: Fetching contextual data from knowledge graph\n\n"

    @staticmethod
    def no_results() -> str:
        return "status: No results found. The query executed successfully but returned no data.\n\n"

    @staticmethod
    def processing_results() -> str:
        return "status: Analyzing results and preparing answer...\n\n"

    @staticmethod
    def error(message: str) -> str:
        return f"Error: {message}\n\n"


@router.post("/query")
async def natural_language_query(request: NLQueryRequest):
    """
    Process a natural language query through the full pipeline:
    1. Convert NL to SPARQL
    2. Execute SPARQL against Virtuoso
    3. Contextualize results with LLM
    4. Stream the final answer

    Returns a Server-Sent Events stream with status updates and final answer.
    """
    with tracer.start_as_current_span("nl_query_pipeline") as span:
        span.set_attribute("query", request.query)
        span.set_attribute("has_context", bool(request.context))

        async def response_stream():
            try:
                # Status: Processing query
                yield f"{StatusMessage.processing_query()}"

                # Get clients
                ollama = get_ollama_client()
                virtuoso = VirtuosoClient()

                # Check if Ollama service is available
                is_healthy = await ollama.health_check()
                if not is_healthy:
                    error_msg = StatusMessage.error("Ollama service is not available")
                    yield f"{error_msg}\n"
                    yield "data: [DONE]\n"
                    return

                # Build the user query
                user_query = request.query
                if request.context:
                    user_query = f"{request.context}\n\n{request.query}"

                # Stage 1: Convert NL to SPARQL
                span.set_attribute("stage", "nl_to_sparql")
                yield f"{StatusMessage.generating_sparql()}"

                try:
                    sparql_query = await ollama.nl_to_sparql(natural_query=user_query)
                    logger.info(f"Generated SPARQL: {sparql_query}")
                    span.set_attribute("sparql_query", sparql_query)

                except Exception as e:
                    logger.error(f"SPARQL generation error: {e}", exc_info=True)
                    error_msg = StatusMessage.error(f"Failed to generate query: {str(e)}")
                    yield f"{error_msg}\n"
                    yield "data: [DONE]\n"
                    return

                # Stage 2: Execute SPARQL query
                span.set_attribute("stage", "execute_sparql")
                yield f"{StatusMessage.executing_query()}"

                try:
                    sparql_results = await virtuoso.execute_query(sparql_query)

                    # Check if we got results
                    result_count = 0
                    if sparql_results.get('results', {}).get('bindings'):
                        result_count = len(sparql_results['results']['bindings'])
                    elif sparql_results.get('boolean') is not None:
                        result_count = 1

                    span.set_attribute("result_count", result_count)
                    logger.info(f"SPARQL query returned {result_count} results")
                    logger.info(f"    SPARQL query results: {sparql_results}")

                    if result_count == 0:
                        yield f"{StatusMessage.no_results()}"
                        yield "data: [DONE]\n"
                        return

                except Exception as e:
                    logger.error(f"SPARQL execution error: {e}", exc_info=True)
                    error_msg = StatusMessage.error(f"Failed to execute query: {str(e)}")
                    yield f"{error_msg}\n"
                    yield "data: [DONE]\n"
                    return

                # Stage 3: Contextualize results with LLM
                span.set_attribute("stage", "contextualize")
                yield f"{StatusMessage.processing_results()}"

                try:
                    # Stream the contextualized answer
                    async for chunk in ollama.contextualize_answer(
                        user_query=request.query,
                        sparql_query=sparql_query,
                        sparql_results=sparql_results,
                        system_prompt=""
                    ):
                        yield f"{chunk}\n"

                    span.set_attribute("stage", "completed")

                except Exception as e:
                    logger.error(f"Contextualization error: {e}", exc_info=True)
                    error_msg = StatusMessage.error(f"Failed to process results: {str(e)}")
                    yield f"{error_msg}\n"
                    yield "data: [DONE]\n"
                    return

                # Completion signal
                yield "data: [DONE]\n"

            except Exception as e:
                logger.error(f"Pipeline error: {e}", exc_info=True)
                error_msg = StatusMessage.error(f"Unexpected error: {str(e)}")
                yield f"{error_msg}\n"
                yield "data: [DONE]\n"

        return StreamingResponse(
            response_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no"
            }
        )


@router.get("/health")
async def health_check():
    """Check if the Ollama service is available."""
    try:
        ollama = get_ollama_client()
        is_healthy = await ollama.health_check()

        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "service": "ollama",
            "models": {
                "llm_model": ollama.llm_model
            }
        }

    except Exception as e:
        logger.error(f"Health check error: {e}")
        return {
            "status": "error",
            "service": "ollama",
            "error": str(e)
        }